package memory

import (
	"errors"
	"os"

	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
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
}

// Create initializes a new, incomplete blob, reserves space for it, and returns a handle to it.
// Incomplete entries cannot be automatically evicted. MarkComplete must be called once the blob is complete.
// The store uses `sizeBytes` for its eviction logic even if the blob's real size differs (which is tolerated).
func (s *Store) Create(key string, sizeBytes uint64) (storelib.FileReadWriter, error) {
	return nil, nil
}

// Open returns a handle to the blob. The handle returns [ErrEvicted] once the blob gets evicted.
func (s *Store) Open(key string) (storelib.FileReadWriter, error) { return nil, nil }

// Delete removes a blob and its metadata from the store. Returns [os.ErrNotExist] on missing entry.
func (s *Store) Delete(key string) error { return nil }

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

// DeleteMetadata removes any metadata of a blob with `md`'s suffix, if present.
func (s *Store) DeleteMetadata(key string, mdSuffix string) error { return nil }

// ScopeComplete scopes [Store]'s APIs such that they can only operate on complete blobs.
// [ErrOutOfScope] is returned if the user tries to operate on an incomplete blob.
func (s *Store) ScopeComplete() *Store { return nil }

// ScopeIncomplete scopes [Store]'s APIs such that they can only operate on incomplete blobs.
// [ErrOutOfScope] is returned if the user tries to operate on a complete blob.
func (s *Store) ScopeIncomplete() *Store { return nil }
