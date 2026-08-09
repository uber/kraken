package tiered

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"

	"github.com/uber-go/tally"
	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/lib/store/memory"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/log"
	"go.uber.org/zap"
)

type store struct {
	disk    *disk.Store
	mem     *memory.Store
	flusher *flusher
	log     *zap.SugaredLogger
	mu      sync.RWMutex
}

func newStore(config *Config, metrics tally.Scope) (*store, *disk.Store, error) {
	err := config.applyDefaults()
	if err != nil {
		return nil, nil, err
	}
	memStore, err := memory.NewStore(config.MemConfig, metrics)
	if err != nil {
		return nil, nil, fmt.Errorf("new mem store: %w", err)
	}
	diskStore, err := disk.NewStore(config.DiskConfig, metrics)
	if err != nil {
		return nil, nil, fmt.Errorf("new disk store: %w", err)
	}

	log := log.Default().With("module", "tiered_store")

	log.Info("Initialized a new tiered.Store")
	return &store{
		disk:    diskStore,
		mem:     memStore,
		flusher: newFlusher(memStore, diskStore, log, config.NumFlushWorkers),
		log:     log,
	}, diskStore, nil
}

func (s *store) Create(key string, sizeBytes uint64) (*File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, inMem := s.mem.Has(key)
	_, inDisk := s.disk.Has(key)
	if inMem || inDisk {
		return nil, os.ErrExist
	}

	memF, err := s.mem.Create(key, sizeBytes)
	if err != nil && !errors.Is(err, memory.ErrNoSpace) {
		return nil, fmt.Errorf("mem store create: %w", err)
	}
	if err == nil {
		return newFile(key, memF, nil, s.disk, s.log), nil
	}

	diskF, err := s.disk.Create(key, sizeBytes)
	if err != nil {
		return nil, fmt.Errorf("disk store create: %w", err)
	}
	return newFile(key, nil, diskF, s.disk, s.log), nil
}

func (s *store) Open(key string, scope storelib.BlobScope) (*File, error) {
	memF, err := s.mem.Scoped(scope).Open(key)
	if errors.Is(err, storelib.ErrOutOfScope) {
		return nil, storelib.ErrOutOfScope
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("mem store open: %w", err)
	}
	if err == nil {
		return newFile(key, memF, nil, s.disk, s.log), nil
	}

	diskF, err := s.disk.Scoped(scope).Open(key)
	if errors.Is(err, storelib.ErrOutOfScope) {
		return nil, storelib.ErrOutOfScope
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, fmt.Errorf("disk store open: %w", err)
	}
	return newFile(key, nil, diskF, s.disk, s.log), nil
}

func (s *store) Has(key string, scope storelib.BlobScope) (inStore bool, inScope bool) {
	inMem, inMemScope := s.mem.Scoped(scope).Has(key)
	if inMem {
		return true, inMemScope
	}

	inDisk, inDiskScope := s.disk.Scoped(scope).Has(key)
	if inDisk {
		return true, inDiskScope
	}
	return false, false
}

func (s *store) Delete(key string, scope storelib.BlobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.mem.Scoped(scope).Delete(key)
	if errors.Is(err, storelib.ErrOutOfScope) {
		return storelib.ErrOutOfScope
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("mem store delete: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		return s.disk.Scoped(scope).Delete(key)
	}

	s.flusher.abort(key)
	err = s.disk.Delete(key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		err = fmt.Errorf("disk store delete: %w", err)
		s.log.With(
			"key", key,
			"error", err).
			Error("Could not delete blob from disk, blob might be leaked and/or in corrupt state")
		return err
	}

	return nil
}

func (s *store) List(scope storelib.BlobScope) []string {
	diskKeys := s.disk.Scoped(scope).List()
	memKeys := s.mem.Scoped(scope).List()

	dedup := make(map[string]struct{})
	for _, key := range diskKeys {
		dedup[key] = struct{}{}
	}
	for _, key := range memKeys {
		dedup[key] = struct{}{}
	}
	if scope == storelib.BlobScopeIncomplete {
		// In the code above, we might have added accidentally entries that are complete from the store's POV,
		// but still incomplete in disk, as the entry is currently being flushed from mem to disk. We remove these entries.
		for _, key := range s.mem.ScopeComplete().List() {
			delete(dedup, key)
		}
	}
	return slices.Collect(maps.Keys(dedup))
}

func (s *store) Stat(key string, scope storelib.BlobScope) (size int64, err error) {
	size, err = s.mem.Scoped(scope).Stat(key)
	if errors.Is(err, storelib.ErrOutOfScope) {
		return 0, storelib.ErrOutOfScope
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("mem store stat: %w", err)
	}
	if err == nil {
		return size, nil
	}

	fi, err := s.disk.Scoped(scope).Stat(key)
	if errors.Is(err, storelib.ErrOutOfScope) {
		return 0, storelib.ErrOutOfScope
	}
	if errors.Is(err, os.ErrNotExist) {
		return 0, os.ErrNotExist
	}
	if err != nil {
		return 0, fmt.Errorf("disk store stat: %w", err)
	}
	return fi.Size(), nil
}

func (s *store) MarkComplete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.mem.ScopeComplete().Has(key); ok {
		return nil // no-op
	}
	if _, ok := s.disk.ScopeComplete().Has(key); ok {
		return nil // no-op
	}

	err := s.mem.BanEviction(key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("mem store ban eviction: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		return s.disk.MarkComplete(key)
	}

	err = s.mem.MarkComplete(key)
	if err != nil {
		return fmt.Errorf("mem store mark complete: %w", err)
	}
	size, err := s.mem.Stat(key)
	if err != nil {
		return fmt.Errorf("mem store stat: %w", err)
	}
	s.flusher.markDirty(key, uint64(size))
	return nil
}

func (s *store) SetMetadata(key string, md metadata.Metadata, scope storelib.BlobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.mem.Scoped(scope).BanEviction(key)
	if errors.Is(err, storelib.ErrOutOfScope) {
		return storelib.ErrOutOfScope
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("mem store ban eviction: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		return s.disk.Scoped(scope).SetMetadata(key, md)
	}

	err = s.mem.SetMetadata(key, md)
	if err != nil {
		return fmt.Errorf("mem store set metadata: %w", err)
	}
	s.flusher.markMetadataDirty(key, md.GetSuffix())
	return nil
}

func (s *store) GetMetadata(key string, md metadata.Metadata, scope storelib.BlobScope) (ok bool, err error) {
	ok, err = s.mem.Scoped(scope).GetMetadata(key, md)
	if errors.Is(err, storelib.ErrOutOfScope) {
		return false, storelib.ErrOutOfScope
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("mem store get metadarta: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		return s.disk.Scoped(scope).GetMetadata(key, md)
	}
	return ok, nil
}

func (s *store) DeleteMetadata(key string, mdSuffix string, scope storelib.BlobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.mem.Scoped(scope).BanEviction(key)
	if errors.Is(err, storelib.ErrOutOfScope) {
		return storelib.ErrOutOfScope
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("mem store ban eviction: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		return s.disk.Scoped(scope).DeleteMetadata(key, mdSuffix)
	}

	err = s.mem.DeleteMetadata(key, mdSuffix)
	if err != nil {
		return fmt.Errorf("mem store delete metadata: %w", err)
	}
	s.flusher.markMetadataDirty(key, mdSuffix)
	return nil
}
