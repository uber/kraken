package tiered

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/lib/store/memory"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/log"
	"go.uber.org/zap"
)

func NewStore(diskConfig *disk.Config, memCapacity uint64, numWorkers int, metrics tally.Scope) (*Store, error) {
	mem, err := memory.NewStore(memCapacity, metrics)
	if err != nil {
		return nil, fmt.Errorf("new mem store: %w", err)
	}
	disk, err := disk.NewStore(diskConfig, metrics)
	if err != nil {
		return nil, fmt.Errorf("new disk store: %w", err)
	}

	log := log.Default().With("module", "tiered_store")

	return &Store{
		disk:    disk,
		mem:     mem,
		flusher: newFlusher(mem, disk, log, numWorkers),
		log:     log,
	}, nil
}

type Store struct {
	disk    *disk.Store
	mem     *memory.Store
	flusher *flusher
	log     *zap.SugaredLogger
	mu      sync.Mutex
}

func (s *Store) Create(key string, sizeBytes uint64) (*File, error) {
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

func (s *Store) Open(key string) (*File, error) {
	memF, err := s.mem.Open(key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("mem store open: %w", err)
	}
	if err == nil {
		return newFile(key, memF, nil, s.disk, s.log), nil
	}

	diskF, err := s.disk.Open(key)
	if errors.Is(err, os.ErrNotExist) {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, fmt.Errorf("disk store open: %w", err)
	}
	return newFile(key, nil, diskF, s.disk, s.log), nil
}

func (s *Store) Has(key string) (inStore bool, inScope bool) {
	inMemStore, inScope := s.mem.Has(key)
	if inMemStore {
		return inMemStore, inScope
	}
	return s.disk.Has(key)
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.mem.Delete(key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("mem store delete: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		return s.disk.Delete(key)
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

func (s *Store) List() []string {
	diskKeys := s.disk.List()
	memKeys := s.mem.List()

	dedup := make(map[string]struct{})
	for _, key := range diskKeys {
		dedup[key] = struct{}{}
	}
	for _, key := range memKeys {
		dedup[key] = struct{}{}
	}
	return slices.Collect(maps.Keys(dedup))
}

func (s *Store) Stat(key string) (size int64, err error) {
	size, err = s.mem.Stat(key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("mem store stat: %w", err)
	}
	if err == nil {
		return size, nil
	}

	fi, err := s.disk.Stat(key)
	if errors.Is(err, os.ErrNotExist) {
		return 0, os.ErrNotExist
	}
	if err != nil {
		return 0, fmt.Errorf("disk store stat: %w", err)
	}
	return fi.Size(), nil
}

func (s *Store) MarkComplete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

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

func (s *Store) SetMetadata(key string, md metadata.Metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.mem.BanEviction(key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("mem store ban eviction: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		return s.disk.SetMetadata(key, md)
	}

	err = s.mem.SetMetadata(key, md)
	if err != nil {
		return fmt.Errorf("mem store set metadata: %w", err)
	}
	s.flusher.markMetadataDirty(key, md.GetSuffix())
	return nil
}

func (s *Store) GetMetadata(key string, md metadata.Metadata) (ok bool, err error) {
	ok, err = s.mem.GetMetadata(key, md)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("mem store get metadarta: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		return s.disk.GetMetadata(key, md)
	}
	return ok, nil
}

func (s *Store) DeleteMetadata(key string, mdSuffix string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.mem.BanEviction(key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("mem store ban eviction: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		return s.disk.DeleteMetadata(key, mdSuffix)
	}

	err = s.mem.DeleteMetadata(key, mdSuffix)
	if err != nil {
		return fmt.Errorf("mem store delete metadata: %w", err)
	}
	s.flusher.markMetadataDirty(key, mdSuffix)
	return nil
}
