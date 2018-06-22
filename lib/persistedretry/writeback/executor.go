package writeback

import (
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/store/metadata"
	"code.uber.internal/infra/kraken/utils/log"
	"github.com/uber-go/tally"
)

// FileStore defines store operations required for write-back.
type FileStore interface {
	DeleteCacheFileMetadata(name string, md metadata.Metadata) error
	GetCacheFileReader(name string) (store.FileReader, error)
}

// Executor executes write back tasks.
type Executor struct {
	stats    tally.Scope
	fs       FileStore
	backends *backend.Manager
}

// NewExecutor creates a new Executor.
func NewExecutor(
	stats tally.Scope,
	fs FileStore,
	backends *backend.Manager) *Executor {

	stats = stats.Tagged(map[string]string{
		"module": "writebackexecutor",
	})

	return &Executor{stats, fs, backends}
}

// Name returns the executor name.
func (e *Executor) Name() string {
	return "writeback"
}

// Exec uploads the cache file corresponding to r's digest to the remote backend
// that matches r's namespace.
func (e *Executor) Exec(r persistedretry.Task) error {
	t := r.(*Task)
	if err := e.upload(t.Namespace, t.Name); err != nil {
		return err
	}
	err := e.fs.DeleteCacheFileMetadata(t.Name, &metadata.Persist{})
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete persist metadata: %s", err)
	}
	return nil
}

func (e *Executor) upload(namespace, name string) error {
	client, err := e.backends.GetClient(namespace)
	if err != nil {
		if err == backend.ErrNamespaceNotFound {
			log.With(
				"namespace", namespace,
				"name", name).Info("Dropping writeback for unconfigured namespace")
			return nil
		}
		return fmt.Errorf("get client: %s", err)
	}

	if _, err := client.Stat(name); err == nil {
		// File already uploaded, no-op.
		return nil
	}

	f, err := e.fs.GetCacheFileReader(name)
	if err != nil {
		if os.IsNotExist(err) {
			// Nothing we can do about this but make noise and drop the task.
			e.stats.Counter("missing_files").Inc(1)
			log.With("name", name).Error("Invariant violation: writeback cache file missing")
			return nil
		}
		return fmt.Errorf("get file: %s", err)
	}
	defer f.Close()

	if err := client.Upload(name, f); err != nil {
		return fmt.Errorf("upload: %s", err)
	}

	return nil
}
