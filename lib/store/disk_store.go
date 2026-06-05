package store

import (
	"errors"
	"os"

	"github.com/uber/kraken/lib/store/metadata"
)

// DiskStore is a content-addressable, persistent, LRU store for blobs and their [metadata.Metadata].
// It is designed for immutable data, i.e. once a blob is stored, it can't be mutated anymore (its metadata can be).
type DiskStore struct {
}

// NewDiskStore creates a new [DiskStore].
func NewDiskStore(capacityBytes int64, dir string) (*DiskStore, error) {
	// TODO - recover persisted state.
	return nil, errors.New("not implemented")
}

// Returns os.ErrNotExist if the blob is not present.
// TODO - decide what happens if the blob is being writtem atm - should return a special error `InProgress` or keep returning `ErrNotExists`?
// TODO - decide what is done when a blob is evicted while io.Reader is held by the client (check what's done right now and evaluate if we should mimic).
func (c *DiskStore) Get(key string) (FileReader, error)

// Stat returns information about the blob.
func (c *DiskStore) Stat(key string) (os.FileInfo, error)

// TODO - we probably want some TTI on uploads to the store, after which we cancel the upload, e.g. 1min without uploading more data here.
// TODO - cannot evict blob until Commit is called.
// If a write for this blob is already in progress, an error is returned.
// CommitWrite MUST be called once the blob is fully written to enable reading it.
func (c *DiskStore) StartWrite(key string, sizeBytes int64) (FileReadWriter, error)

// CommitWrite must be called once a blob is fully written to make it visible to clients for reading.
func (c *DiskStore) CommitWrite(key string) error

// CancelWrite should be called if an upload to the store has been started but will not be finished (e.g. due to an error).
func (c *DiskStore) CancelWrite(key string) error

// Delete deletes a blob.
func (c *DiskStore) Delete(key string) error

// List returns the keys of all blobs stored in the store.
func (c *DiskStore) List() ([]string, error)

// ForbidEviction stops a blob from being evicted until [AllowEviction] is called. Needed when blobs must be written back to GCS/S3/etc.
func (c *DiskStore) ForbidEviction(key string) error

// AllowEviction removes the effect of ForbidEviction for a blob.
func (c *DiskStore) AllowEviction(key string) error

// WriteMetadata atomically stores `md` on disk. Can be called both before and after a file has been committed.
func (c *DiskStore) SetMetadata(key string, md metadata.Metadata) error

// GetMetadata populates `md` if the metadata is present. Can be called both before and after a file has been committed.
func (c *DiskStore) GetMetadata(key string, md metadata.Metadata) (err error)

// DeleteMetadata removes the respective metadata, if present.
func (c *DiskStore) DeleteMetadata(key string, md metadata.Metadata) error
