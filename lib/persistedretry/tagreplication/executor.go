package tagreplication

import (
	"fmt"
	"time"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/origin/blobclient"

	"github.com/uber-go/tally"
)

// Executor executes tag replication tasks.
type Executor struct {
	stats             tally.Scope
	originCluster     blobclient.ClusterClient
	tagClientProvider tagclient.Provider
}

// NewExecutor creates a new Executor.
func NewExecutor(
	stats tally.Scope,
	originCluster blobclient.ClusterClient,
	tagClientProvider tagclient.Provider) *Executor {

	stats = stats.Tagged(map[string]string{
		"module": "tagreplicationexecutor",
	})

	return &Executor{stats, originCluster, tagClientProvider}
}

// Name returns the executor name.
func (e *Executor) Name() string {
	return "tagreplication"
}

// Exec replicates a tag's blob dependencies to the task's remote origin
// cluster, then replicates the tag to the remote build-index.
func (e *Executor) Exec(r persistedretry.Task) error {
	t := r.(*Task)
	start := time.Now()
	remoteTagClient := e.tagClientProvider.Provide(t.Destination)

	if ok, err := remoteTagClient.Has(t.Tag); err == nil && ok {
		// Remote index already has the tag, therefore dependencies have already
		// been replicated. No-op.
		return nil
	}

	remoteOrigin, err := remoteTagClient.Origin()
	if err != nil {
		return fmt.Errorf("lookup remote origin cluster: %s", err)
	}
	for _, d := range t.Dependencies {
		if err := e.originCluster.ReplicateToRemote(t.Tag, d, remoteOrigin); err != nil {
			return fmt.Errorf("origin cluster replicate: %s", err)
		}
	}

	if err := remoteTagClient.Put(t.Tag, t.Digest); err != nil {
		return fmt.Errorf("put tag: %s", err)
	}

	// We don't want to time noops nor errors.
	e.stats.Timer("replicate").Record(time.Since(start))
	e.stats.Timer("lifetime").Record(time.Since(t.CreatedAt))

	return nil
}
