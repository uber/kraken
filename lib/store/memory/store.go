package memory

import (
	"container/list"
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"github.com/uber-go/tally"
	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/log"
	"go.uber.org/zap"
)

// ErrNoSpace means the store could not free enough space for a new entry.
var ErrNoSpace error = errors.New("cannot free enough memory for new entry")

// ErrEvicted is returned when a user tries operating on a blob's handle after the blob has been evicted.
var ErrEvicted error = errors.New("the blob has been evicted from the store")

// Store is an in-memory, thread-safe, LRU cache for blobs and their [metadata.Metadata].
//
//   - Supports pagination of blobs during reading/writing, such that blobs don't need to be fully loaded into memory.
//
//   - New blobs are considered 'incomplete', which unlists them from LRU eviction. The store can be scoped to work on only (in-)complete blobs.
//
//   - The store prioritizes writing new blobs over reading existing ones. Therefore, blobs may get evicted while clients hold a [storelib.FileReadWriter] to them.
//     In such cases, [ErrEvicted] is returned.
//
//   - All APIs are thread-safe. Parallel access to a single blob is allowed but clients must ensure they don't intervene with one another.
//
//   - Supports (un-)marking blobs as non-evictable (needed when we want to ensure an entry does not get evicted before the client flushes it to disk).
type Store struct {
	blobs      map[string]*blob
	evictQueue *list.List // front is next to evict (least recently used entry)
	size       uint64
	capacity   uint64
	mu         sync.RWMutex // TODO - benchmark if a [sync.Mutex] has better perf.
	log        *zap.SugaredLogger
	metrics    tally.Scope
}

// NewStore initializes an empty [*Store].
func NewStore(capacityBytes uint64, metrics tally.Scope) (*Store, error) {
	if capacityBytes <= 0 {
		return nil, errors.New("store capacity must be positive")
	}

	log := log.Default().With("module", "memory_store")

	log.Info("Initialized new, empty *memory.Store")
	return &Store{
		blobs:      make(map[string]*blob, 0),
		evictQueue: list.New(),
		capacity:   capacityBytes,
		size:       0,
		log:        log,
		metrics:    metrics,
	}, nil
}

type blob struct {
	data           atomic.Pointer[[]byte] // set to nil upon eviction/deletion.
	node           *list.Element
	mdList         []metadata.Metadata
	size           uint64
	complete       bool
	evictionBanned bool
	sliceMu        sync.RWMutex // Writes to `data`'s array are parallelize-able with each other but NOT with writes that mutate 1) the atomic pointer OR 2) the slice (e.g. resizing the array).
}

// Create initializes a new, incomplete blob, reserves space for it, and returns a handle to it.
// Incomplete entries cannot be automatically evicted. MarkComplete must be called once the blob is complete.
// The store uses `sizeBytes` for its eviction logic even if the blob's real size differs (which is tolerated).
func (s *Store) Create(key string, sizeBytes uint64) (storelib.FileReadWriter, error) {
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
		mdList:         make([]metadata.Metadata, 0),
	}
	arr := make([]byte, sizeBytes)
	b.data.Store(&arr)
	s.blobs[key] = b
	return newHandle(&b.data, &b.sliceMu, s.log), nil
}

func (s *Store) reserveSpace(space uint64) bool {
	// TODO - consider whether it's a worth optimization to check if we can evict enough data BEFORE we start evicting, as to prevent evicting needlessly.
	for s.size+space > s.capacity {
		if s.evictQueue.Len() == 0 {
			return false
		}
		toEvictNode := s.evictQueue.Front()
		toEvictKey := toEvictNode.Value.(string)
		b := s.blobs[toEvictKey]
		b.sliceMu.Lock()
		b.data.Store(nil) // Ensure the byte slice is not referenced by clients outside the store holding [*handle], so GC can evict the memory.
		b.sliceMu.Unlock()
		delete(s.blobs, toEvictKey)
		s.size -= b.size
		s.evictQueue.Remove(toEvictNode)
	}

	s.size += space
	return true
}

// Open returns a handle to the blob. The handle returns [ErrEvicted] once the blob gets evicted.
func (s *Store) Open(key string) (storelib.FileReadWriter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return nil, os.ErrNotExist
	}

	if b.node != nil {
		s.evictQueue.MoveToBack(b.node)
	}
	return newHandle(&b.data, &b.sliceMu, s.log), nil
}

// Delete removes a blob and its metadata from the store. Returns [os.ErrNotExist] on missing entry.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}

	b.sliceMu.Lock()
	b.data.Store(nil) // Ensure the byte slice is not referenced by clients outside the store holding [*handle], so GC can evict the memory.
	b.sliceMu.Unlock()
	delete(s.blobs, key)
	s.evictQueue.Remove(b.node)
	s.size -= b.size
	return nil
}

// List returns the keys of all blobs (except those out of scope).
func (s *Store) List() []string { return nil }

// Stat returns the blob's [os.FileInfo].
func (s *Store) Stat(key string) (os.FileInfo, error) { return nil, nil }

// MarkComplete marks the blob as fully written, which enlists it for LRU eviction (unless BanEviction has been called).
// Additionally, other store APIs may filter blobs based on completeness.
func (s *Store) MarkComplete(key string) error { return nil }

// BanEviction marks a blob as unevictable by LRU eviction. It is idempotent.
// Usually used by clients to ensure a blob is not evicted before being flushed to disk.
func (s *Store) BanEviction(key string) error { return nil }

// UnbanEviction removes the effect of BanEviction for a blob. It is idempotent.
func (s *Store) UnbanEviction(key string) error { return nil }

// SetMetadata sets the respective metadata of the blob.
func (s *Store) SetMetadata(key string, md metadata.Metadata) error { return nil }

// GetMetadata populates `md` if the metadata is present. Returns [os.ErrNotExist] if key is not in store.
func (s *Store) GetMetadata(key string, md metadata.Metadata) (ok bool, err error) { return false, nil }

// ListMetadata returns all [metadata.Metadata] of key.
func (s *Store) ListMetadata(key string) ([]metadata.Metadata, error) { return nil, nil }

// DeleteMetadata removes a blob's metadata, if present.
func (s *Store) DeleteMetadata(key string, mdSuffix string) error { return nil }

// ScopeComplete scopes [Store]'s APIs such that they can only operate on complete blobs.
// [ErrOutOfScope] is returned if the user tries to operate on an incomplete blob.
func (s *Store) ScopeComplete() *Store { return nil }

// ScopeIncomplete scopes [Store]'s APIs such that they can only operate on incomplete blobs.
// [ErrOutOfScope] is returned if the user tries to operate on a complete blob.
func (s *Store) ScopeIncomplete() *Store { return nil }
