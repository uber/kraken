package store

import (
	"errors"
	"os"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/lib/store/metadata"
)

// ErrOutOfScope is returned when the provided key is in the store, but not in the store's [blobScope],
// e.g. if the blob is incomplete, but ScopeComplete was called.
var ErrOutOfScope = errors.New("the blob is in DiskStore but filtered by the selected scope")

// DiskStore is a key-value, persistent, thread-safe, LRU store for blobs and their [metadata.Metadata].
//
//   - Supports pagination of blobs during reading/writing, such that blobs don't need to be fully loaded into memory.
//
//   - New blobs are considered 'incomplete', which unlists them from LRU eviction. The store can be scoped to work on only (in-)complete blobs.
//
//   - All APIs are thread-safe. Parallel access to a single file is allowed but clients must ensure they don't intervene with one another.
//
//   - Supports (un-)marking blobs as non-evictable (may be needed when that data must be written back to remote storage).
//
//   - Crash-resistant - all state is restored upon restart (check [newDiskStore] for details).
//
//   - Uses directory sharding to speed up disk performance.
type DiskStore struct {
	*diskStore
	scope blobScope
}

// NewDiskStore initializes a new [*DiskStore]. If the store has been initialized in the same
// directory before, its state is recovered from disk with the following caveats:
//
//   - `rebootIncompleteBlobs` configures whether incomplete blobs are evicted or rebooted on restart.
//
//   - If the store's size is bigger than its capacity (e.g. configured capacity has been reduced or files have been leaked),
//     it evicts blobs until size is within capacity.
func NewDiskStore(capacityBytes uint64, rootDir string, rebootIncompleteBlobs bool, shardLength int, metrics tally.Scope) (*DiskStore, error) {
	diskStore, err := newDiskStore(capacityBytes, rootDir, rebootIncompleteBlobs, shardLength, metrics)
	if err != nil {
		return nil, err
	}
	return &DiskStore{
		diskStore: diskStore,
		scope:     blobScopeAny,
	}, nil
}

// Create adds a new, incomplete blob to the store and reserves space for it.
// Incomplete entries cannot be automatically evicted. MarkComplete must be called once the blob is complete.
// DiskStore uses `sizeBytes` for its eviction logic even if the blob's real size differs.
func (s *diskStore) Create(key string, sizeBytes uint64) (FileReadWriter, error) {
	return s.create(key, sizeBytes)
}

// Open returns an FD to a file in the store. [os.ErrNotExist] is returned on missing entry.
func (s *DiskStore) Open(key string) (FileReadWriter, error) { return s.open(key, s.scope) }

// Stat returns [os.FileInfo] about the blob. Returns [os.ErrNotExist] if the blob is not found.
func (s *DiskStore) Stat(key string) (os.FileInfo, error) { return s.stat(key, s.scope) }

// MarkComplete marks a blob as fully written. It enlists the blob for LRU eviction (unless BanEviction has been called).
// Additionally, other store APIs may filter blobs based on completeness.
func (s *diskStore) MarkComplete(key string) error { return s.markComplete(key) }

// Delete removes a blob and its [metadata.Metadata] from the store.
func (s *DiskStore) Delete(key string) error { return s.delete(key, s.scope) }

// List returns the keys of all blobs (except those out of scope).
func (s *DiskStore) List() []string { return s.list(s.scope) }

// BanEviction marks a blob as unevictable by LRU eviction. It is idempotent.
// Needed when e.g. blobs must be written back to GCS/S3 and eviction before that is unacceptable.
func (s *DiskStore) BanEviction(key string) error { return s.banEviction(key, s.scope) }

// UnbanEviction removes the effect of BanEviction for a blob. It is idempotent.
func (s *DiskStore) UnbanEviction(key string) error { return s.unbanEviction(key, s.scope) }

// SetMetadata atomically sets the respective metadata for a blob.
func (s *DiskStore) SetMetadata(key string, md metadata.Metadata) error {
	return s.setMetadata(key, md, s.scope)
}

// GetMetadata populates `md` if the metadata is present. Returns [os.ErrNotExist] if key is not in store.
func (s *DiskStore) GetMetadata(key string, md metadata.Metadata) (ok bool, err error) {
	return s.getMetadata(key, md, s.scope)
}

// DeleteMetadata removes any metadata of a blob with `md`'s suffix, if present.
func (s *DiskStore) DeleteMetadata(key string, md metadata.Metadata) error {
	return s.deleteMetadata(key, md, s.scope)
}

// ListMetadata returns all [metadata.Metadata] of key.
func (s *DiskStore) ListMetadata(key string) ([]metadata.Metadata, error) {
	return s.listMetadata(key, s.scope)
}

// WriteAtMetadata implements [io.WriterAt] for the metadata file on disk.
func (s *DiskStore) WriteAtMetadata(key string, md metadata.Metadata, p []byte, off int64) error {
	return s.writeAtMetadata(key, md, p, off, s.scope)
}

// ScopeComplete scopes [DiskStore]'s APIs such that they can only operate on complete blobs.
// [ErrOutOfScope] is returned if the user tries to operate on an incomplete blob.
func (s *DiskStore) ScopeComplete() *DiskStore { return &DiskStore{s.diskStore, blobScopeComplete} }

// ScopeIncomplete scopes [DiskStore]'s APIs such that they can only operate on incomplete blobs.
// [ErrOutOfScope] is returned if the user tries to operate on a complete blob.
func (s *DiskStore) ScopeIncomplete() *DiskStore { return &DiskStore{s.diskStore, blobScopeIncomplete} }
