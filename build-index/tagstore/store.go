package tagstore

import (
	"fmt"
	"io"
	"time"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/lib/persistedretry/tagreplication"
	"code.uber.internal/infra/kraken/lib/persistedretry/writeback"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/store/metadata"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
)

// FileStore defines operations required for storing tags on disk.
type FileStore interface {
	CreateCacheFile(name string, r io.Reader) error
	SetCacheFileMetadata(name string, md metadata.Metadata) (bool, error)
	GetCacheFileReader(name string) (store.FileReader, error)
}

// Store defines tag storage operations.
type Store interface {
	Put(tag string, d core.Digest, writeBackDelay time.Duration) error
	Get(tag string, fallback bool) (core.Digest, error)
}

// tagStore encapsulates all caching / storage for tags. It manages tag storage on
// three levels:
// 1. In-memory cache: used for tag lookups only.
// 2. On-disk file store: persists tags for write-back purposes.
// 3. Remote storage: durable tag storage.
type tagStore struct {
	cache            *dedup.Cache
	fs               FileStore
	backends         *backend.Manager
	writeBackManager persistedretry.Manager

	// For fallback.
	remotes           tagreplication.Remotes
	tagClientProvider tagclient.Provider
}

// New creates a new Store.
func New(
	config Config,
	stats tally.Scope,
	fs FileStore,
	backends *backend.Manager,
	writeBackManager persistedretry.Manager,
	remotes tagreplication.Remotes,
	tagClientProvider tagclient.Provider) Store {

	if config.DisableFallback {
		log.Warn("Fallback disabled for tag storage")
	}

	stats = stats.Tagged(map[string]string{
		"module": "tagstore",
	})

	resolver := &tagResolver{fs, backends, config.DisableFallback, remotes, tagClientProvider}

	cache := dedup.NewCache(config.Cache, clock.New(), resolver)

	return &tagStore{
		cache:             cache,
		fs:                fs,
		backends:          backends,
		writeBackManager:  writeBackManager,
		remotes:           remotes,
		tagClientProvider: tagClientProvider,
	}
}

func (s *tagStore) Put(tag string, d core.Digest, writeBackDelay time.Duration) error {
	if err := writeTagToDisk(tag, d, s.fs); err != nil {
		return fmt.Errorf("write tag to disk: %s", err)
	}
	if _, err := s.fs.SetCacheFileMetadata(tag, metadata.NewPersist(true)); err != nil {
		return fmt.Errorf("set persist metadata: %s", err)
	}
	task := writeback.NewTaskWithDelay(tag, tag, writeBackDelay)
	if err := s.writeBackManager.Add(task); err != nil {
		return fmt.Errorf("add write-back task: %s", err)
	}
	return nil
}

func (s *tagStore) Get(tag string, fallback bool) (core.Digest, error) {
	v, err := s.cache.Get(resolveContext{fallback}, tag)
	if err != nil {
		return core.Digest{}, err
	}
	return v.(core.Digest), nil
}
