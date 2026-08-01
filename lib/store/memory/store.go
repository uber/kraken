package memory

import (
	"container/list"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/uber-go/tally"
	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/log"
	"go.uber.org/zap"
)

// store implements the APIs of [Store]. [store]'s APIs expose the [storelib.BlobScope] arg,
// while [Store]'s APIs omit that arg (cleaner interface) and instead expose other APIs to scope the whole store.
//
// Check [Store]'s comments for details on functionality.
type store struct {
	blobs      map[string]*blob
	evictQueue *list.List // front is next to evict (least recently used entry)
	size       uint64
	capacity   uint64
	mu         sync.RWMutex // TODO - benchmark if a [sync.Mutex] has better perf.
	log        *zap.SugaredLogger
	metrics    tally.Scope
}

type blob struct {
	data           atomic.Pointer[[]byte] // set to nil upon eviction/deletion.
	node           *list.Element
	metadatas      map[string]metadata.Metadata
	size           uint64
	complete       bool
	evictionBanned bool
	sliceMu        sync.Mutex // Synchronizes writes to 1) the atomic pointer and 2) the slice (not the array!).
}

func newStore(capacityBytes uint64, metrics tally.Scope) (*store, error) {
	if capacityBytes <= 0 {
		return nil, errors.New("store capacity must be positive")
	}

	log := log.Default().With("module", "memory_store")

	log.Info("Initialized new, empty memory.Store")
	s := &store{
		blobs:      make(map[string]*blob, 0),
		evictQueue: list.New(),
		capacity:   capacityBytes,
		size:       0,
		log:        log,
		metrics:    metrics,
	}

	s.emitUsageMetrics()
	return s, nil
}

func (s *store) Create(key string, sizeBytes uint64) (*File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.blobs[key]
	if ok {
		return nil, os.ErrExist
	}
	if ok := s.reserveSpace(sizeBytes); !ok {
		return nil, ErrNoSpace
	}
	b := &blob{
		size:           sizeBytes,
		complete:       false,
		evictionBanned: false,
		node:           nil,
		metadatas:      make(map[string]metadata.Metadata, 0),
	}
	arr := make([]byte, 0, sizeBytes)
	b.data.Store(&arr)
	s.blobs[key] = b

	s.emitUsageMetrics()
	return newFile(&b.data, &b.sliceMu), nil
}

func (s *store) reserveSpace(space uint64) bool {
	// TODO - consider whether it's a worth optimization to check if we can evict enough data BEFORE we start evicting, as to prevent evicting needlessly.
	for s.size+space > s.capacity {
		if s.evictQueue.Len() == 0 {
			return false
		}
		toEvictNode := s.evictQueue.Front()
		toEvictKey := toEvictNode.Value.(string) //nolint:errcheck
		b := s.blobs[toEvictKey]
		b.sliceMu.Lock()
		b.data.Store(nil) // Ensure the byte slice is not referenced by clients outside the store holding [*File], so GC can evict the memory.
		b.sliceMu.Unlock()
		delete(s.blobs, toEvictKey)
		s.evictQueue.Remove(toEvictNode)
		s.releaseSpace(b.size)
	}

	s.size += space
	return true
}

func (s *store) releaseSpace(space uint64) {
	if space > s.size {
		s.log.Error("Invariant violation - memory.Store wants to release more space than actually reserved. Failing open by setting store.size = 0")
		s.size = 0
		return
	}
	s.size -= space
}

func (s *store) Open(key string, scope storelib.BlobScope) (*File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return nil, err
	}

	if b.node != nil {
		s.evictQueue.MoveToBack(b.node)
	}
	return newFile(&b.data, &b.sliceMu), nil
}

func (s *store) Has(key string, scope storelib.BlobScope) (inStore bool, inScope bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	b, ok := s.blobs[key]
	if !ok {
		return false, false
	}
	if err := isOutOfScope(b, scope); err != nil {
		return true, false
	}
	return true, true
}

func (s *store) Delete(key string, scope storelib.BlobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return err
	}

	b.sliceMu.Lock()
	b.data.Store(nil) // Ensure the byte slice is not referenced by clients outside the store holding [*File], so GC can evict the memory.
	b.sliceMu.Unlock()
	delete(s.blobs, key)
	if b.node != nil {
		s.evictQueue.Remove(b.node)
	}
	s.releaseSpace(b.size)

	s.emitUsageMetrics()
	return nil
}

func (s *store) List(scope storelib.BlobScope) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]string, 0, len(s.blobs))
	for key, b := range s.blobs {
		if err := isOutOfScope(b, scope); err != nil {
			continue
		}
		res = append(res, key)
	}
	return res
}

func (s *store) Stat(key string, scope storelib.BlobScope) (size int64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	b, ok := s.blobs[key]
	if !ok {
		return 0, os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return 0, err
	}
	buf := *b.data.Load()
	return int64(len(buf)), nil
}

func (s *store) MarkComplete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}
	if b.complete {
		// no-op
		return nil
	}

	b.complete = true
	if !b.evictionBanned {
		node := s.evictQueue.PushBack(key)
		b.node = node
	}
	for mdSuffix, md := range b.metadatas {
		if !md.Movable() {
			delete(b.metadatas, mdSuffix)
		}
	}
	return nil
}

func (s *store) BanEviction(key string, scope storelib.BlobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return err
	}
	if b.evictionBanned {
		// no-op
		return nil
	}

	b.evictionBanned = true
	if b.complete {
		s.evictQueue.Remove(b.node)
		b.node = nil
	}
	return nil
}

func (s *store) UnbanEviction(key string, scope storelib.BlobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return err
	}
	if !b.evictionBanned {
		// no-op
		return nil
	}

	b.evictionBanned = false
	if b.complete {
		node := s.evictQueue.PushBack(key)
		b.node = node
	}
	return nil
}

func (s *store) SetMetadata(key string, md metadata.Metadata, scope storelib.BlobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return err
	}
	b.metadatas[md.GetSuffix()] = md
	return nil
}

func (s *store) GetMetadata(key string, md metadata.Metadata, scope storelib.BlobScope) (ok bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	b, ok := s.blobs[key]
	if !ok {
		return false, os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return false, err
	}

	res, ok := b.metadatas[md.GetSuffix()]
	if !ok {
		return false, nil
	}
	mdData, err := res.Serialize()
	if err != nil {
		return false, fmt.Errorf("serialize metadata: %w", err)
	}
	err = md.Deserialize(mdData)
	if err != nil {
		return false, fmt.Errorf("deserialize metadata: %w", err)
	}
	return ok, nil
}

func (s *store) ListMetadata(key string, scope storelib.BlobScope) ([]metadata.Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	b, ok := s.blobs[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return nil, err
	}

	return slices.Collect(maps.Values(b.metadatas)), nil
}

func (s *store) DeleteMetadata(key string, mdSuffix string, scope storelib.BlobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return err
	}

	delete(b.metadatas, mdSuffix)
	return nil
}

func isOutOfScope(b *blob, scope storelib.BlobScope) error {
	if (b.complete && scope == storelib.BlobScopeIncomplete) || (!b.complete && scope == storelib.BlobScopeComplete) {
		return storelib.ErrOutOfScope
	}
	return nil
}

func (s *store) emitUsageMetrics() {
	s.metrics.Gauge("num_entries").Update(float64(len(s.blobs)))
	s.metrics.Gauge("size_bytes").Update(float64(s.size))
}
