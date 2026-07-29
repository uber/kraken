package disk

import (
	"os"

	"github.com/uber-go/tally"
	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
)

// Store is a key-value, persistent, thread-safe, LRU store for blobs and their [metadata.Metadata].
//
//   - Supports pagination of blobs during reading/writing, such that blobs don't need to be fully loaded into memory.
//
//   - New blobs are considered 'incomplete', which unlists them from LRU eviction. The store can be scoped to work on only (in-)complete blobs.
//
//   - All APIs are thread-safe. Parallel access to a single file is allowed but clients must ensure they don't intervene with one another.
//
//   - Supports (un-)marking blobs as non-evictable (may be needed when that data must be written back to remote storage).
//
//   - Crash-resistant - all state is restored upon restart (check [newStore] for details).
//
//   - Supports directory sharding to speed up disk performance.
type Store struct {
	impl  *store
	scope storelib.BlobScope
}

// NewStore initializes a new [*Store]. If the store has been initialized in the same
// directory before, its state is recovered from disk with the following caveats:
//
//   - `rebootIncompleteBlobs` configures whether incomplete blobs are evicted or rebooted on restart.
//
//   - If the store's size is bigger than its capacity (e.g. configured capacity has been reduced or files have been leaked),
//     it evicts blobs until size is within capacity.
func NewStore(config *Config, metrics tally.Scope) (*Store, error) {
	s, err := newStore(config, metrics)
	if err != nil {
		return nil, err
	}
	return &Store{
		impl:  s,
		scope: storelib.BlobScopeAny,
	}, nil
}

// Create adds a new, incomplete blob to the store and reserves space for it.
// Incomplete entries cannot be automatically evicted. MarkComplete must be called once the blob is complete.
// The store uses `sizeBytes` for its eviction logic even if the blob's real size differs.
func (s *Store) Create(key string, sizeBytes uint64) (*File, error) {
	return s.impl.Create(key, sizeBytes)
}

// Open returns an FD to a file in the store. [os.ErrNotExist] is returned on missing entry.
func (s *Store) Open(key string) (*File, error) { return s.impl.Open(key, s.scope) }

// Has checks if the blob is in the store.
func (s *Store) Has(key string) (inStore bool, inScope bool) { return s.impl.Has(key, s.scope) }

// Stat returns [os.FileInfo] about the blob. Returns [os.ErrNotExist] if the blob is not found.
func (s *Store) Stat(key string) (os.FileInfo, error) { return s.impl.Stat(key, s.scope) }

// MarkComplete marks a blob as fully written. It enlists the blob for LRU eviction (unless BanEviction has been called).
// Additionally, other store APIs may filter blobs based on completeness.
func (s *Store) MarkComplete(key string) error { return s.impl.MarkComplete(key) }

// Delete removes a blob and its [metadata.Metadata] from the store. Returns [os.ErrNotExist] on missing blob.
func (s *Store) Delete(key string) error { return s.impl.Delete(key, s.scope) }

// List returns the keys of all blobs (except those out of scope).
func (s *Store) List() []string { return s.impl.list(s.scope) }

// BanEviction marks a blob as unevictable by LRU eviction. It is idempotent.
// Needed when e.g. blobs must be written back to GCS/S3 and eviction before that is unacceptable.
func (s *Store) BanEviction(key string) error { return s.impl.BanEviction(key, s.scope) }

// UnbanEviction removes the effect of BanEviction for a blob. It is idempotent.
func (s *Store) UnbanEviction(key string) error { return s.impl.UnbanEviction(key, s.scope) }

// SetMetadata atomically sets the respective metadata for a blob.
func (s *Store) SetMetadata(key string, md metadata.Metadata) error {
	return s.impl.SetMetadata(key, md, s.scope)
}

// GetMetadata populates `md` if the metadata is present. Returns [os.ErrNotExist] if key is not in store.
func (s *Store) GetMetadata(key string, md metadata.Metadata) (ok bool, err error) {
	return s.impl.GetMetadata(key, md, s.scope)
}

// DeleteMetadata removes a blob's metadata. No error returned if the metadata is not present.
func (s *Store) DeleteMetadata(key string, md metadata.Metadata) error {
	return s.impl.DeleteMetadata(key, md, s.scope)
}

// ListMetadata returns all [metadata.Metadata] of key.
func (s *Store) ListMetadata(key string) ([]metadata.Metadata, error) {
	return s.impl.ListMetadata(key, s.scope)
}

// WriteAtMetadata implements [io.WriterAt] for the metadata file on disk.
func (s *Store) WriteAtMetadata(key string, md metadata.Metadata, p []byte, off int64) error {
	return s.impl.WriteAtMetadata(key, md, p, off, s.scope)
}

// ScopeComplete scopes [Store]'s APIs such that they can only operate on complete blobs (except MarkComplete and Create).
// [storelib.ErrOutOfScope] is returned if the user tries to operate on an incomplete blob.
func (s *Store) ScopeComplete() *Store { return &Store{s.impl, storelib.BlobScopeComplete} }

// ScopeIncomplete scopes [Store]'s APIs such that they can only operate on incomplete blobs.
// [storelib.ErrOutOfScope] is returned if the user tries to operate on a complete blob.
func (s *Store) ScopeIncomplete() *Store { return &Store{s.impl, storelib.BlobScopeIncomplete} }
