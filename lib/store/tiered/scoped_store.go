package tiered

import (
	"github.com/uber-go/tally"
	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/lib/store/metadata"
)

// Store is a tiered (disk + memory), thread-safe, LRU cache for blobs and their [metadata.Metadata].
//
//   - New blobs are initially created in the memory cache to speed up writes/reads and asynchronously flushed to disk
//     once MarkComplete is called on them. If the memory cache is full with inevctable blobs, the new blob is created on disk.
//
//   - Partially crash-resistant - all complete blobs that were fully flushed to disk are persisted. Use [disk.Store] if you need full crash resistence.
//
//   - Supports pagination of blobs during reading/writing, such that blobs don't need to be fully loaded into memory by the client.
//
//   - New blobs are considered 'incomplete', which unlists them from automatic LRU eviction. The store can be scoped to work on only (in-)complete blobs.
//
//   - All APIs are thread-safe, including operating on a blob in parallel.
type Store struct {
	impl  *store
	scope storelib.BlobScope
}

// NewStore creates a new [Store].
func NewStore(diskConfig *disk.Config, memCapacity uint64, numWorkers int, metrics tally.Scope) (*Store, error) {
	s, err := newStore(diskConfig, memCapacity, numWorkers, metrics)
	if err != nil {
		return nil, err
	}
	return &Store{
		impl:  s,
		scope: storelib.BlobScopeAny,
	}, nil
}

// Create initializes a new, incomplete blob, reserves space for it, and returns a [*File] pointing to it.
// Incomplete entries cannot be automatically evicted. MarkComplete must be called once the blob is complete.
// The store uses `sizeBytes` for its eviction logic even if the blob's real size differs (which is tolerated).
func (s *Store) Create(key string, sizeBytes uint64) (*File, error) {
	return s.impl.Create(key, sizeBytes)
}

// Open returns a [*File] pointing to the blob.
func (s *Store) Open(key string) (*File, error) { return s.impl.Open(key, s.scope) }

// Has reports if the blob is 1) in the store and 2) in scope.
// If you don't care about the blob's scope and just want to check membership, do:
//
//	_, ok := store.Has(key)
func (s *Store) Has(key string) (inStore bool, inScope bool) { return s.impl.Has(key, s.scope) }

// Stat returns the blob's actual size, even if it differs from the size reported when Create was called.
func (s *Store) Stat(key string) (size int64, err error) { return s.impl.Stat(key, s.scope) }

// MarkComplete MUST be called once a blob is fully written, as it
// 1) enqueues it to get flushed to disk (after which the blob is eligible for eviction from the memory store)
// 2) enlists it for LRU eviction (unless BanEviction has been called).
// Additionally, other store APIs may filter blobs based on completeness. No-op if the blob is already complete
func (s *Store) MarkComplete(key string) error { return s.impl.MarkComplete(key) }

// Delete removes a blob and its metadata from the store.
func (s *Store) Delete(key string) error { return s.impl.Delete(key, s.scope) }

// List returns the keys of all blobs (except those out of scope).
func (s *Store) List() []string { return s.impl.List(s.scope) }

// SetMetadata sets the respective metadata of the blob.
func (s *Store) SetMetadata(key string, md metadata.Metadata) error {
	return s.impl.SetMetadata(key, md, s.scope)
}

// GetMetadata populates `md` if the metadata is present.
func (s *Store) GetMetadata(key string, md metadata.Metadata) (ok bool, err error) {
	return s.impl.GetMetadata(key, md, s.scope)
}

// DeleteMetadata removes a blob's metadata. No-op if the md is not present.
func (s *Store) DeleteMetadata(key string, mdSuffix string) error {
	return s.impl.DeleteMetadata(key, mdSuffix, s.scope)
}

// ScopeComplete scopes [Store]'s APIs such that they can only operate on complete blobs.
// [storelib.ErrOutOfScope] is returned if the user tries to operate on an incomplete blob.
func (s *Store) ScopeComplete() *Store { return &Store{s.impl, storelib.BlobScopeComplete} }

// ScopeIncomplete scopes [Store]'s APIs such that they can only operate on incomplete blobs.
// [storelib.ErrOutOfScope] is returned if the user tries to operate on a complete blob.
func (s *Store) ScopeIncomplete() *Store { return &Store{s.impl, storelib.BlobScopeIncomplete} }
