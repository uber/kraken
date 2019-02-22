package base

import (
	"github.com/andres-erbsen/clock"
)

// FileStore manages files and their metadata. Actual operations are done through FileOp.
type FileStore interface {
	NewFileOp() FileOp
}

// localFileStore manages all agent files on local disk.
type localFileStore struct {
	fileEntryFactory FileEntryFactory
	fileMap          FileMap
}

// NewLocalFileStore initializes and returns a new FileStore.
func NewLocalFileStore(clk clock.Clock) FileStore {
	m := NewLATFileMap(clk)
	return &localFileStore{
		fileEntryFactory: NewLocalFileEntryFactory(),
		fileMap:          m,
	}
}

// NewCASFileStore initializes and returns a new Content-Addressable FileStore.
// It uses the first few bytes of file digest (which is also used as file name)
// as shard ID.
// For every byte, one more level of directories will be created.
func NewCASFileStore(clk clock.Clock) FileStore {
	m := NewLATFileMap(clk)
	return &localFileStore{
		fileEntryFactory: NewCASFileEntryFactory(),
		fileMap:          m,
	}
}

// NewLRUFileStore initializes and returns a new LRU FileStore.
// When size exceeds limit, the least recently accessed entry will be removed.
func NewLRUFileStore(size int, clk clock.Clock) FileStore {
	m := NewLRUFileMap(size, clk)
	return &localFileStore{
		fileEntryFactory: NewLocalFileEntryFactory(),
		fileMap:          m,
	}
}

// NewCASFileStoreWithLRUMap initializes and returns a new Content-Addressable
// FileStore. It uses the first few bytes of file digest (which is also used as
// file name) as shard ID.
// For every byte, one more level of directories will be created. It also stores
// objects in a LRU FileStore.
// When size exceeds limit, the least recently accessed entry will be removed.
func NewCASFileStoreWithLRUMap(size int, clk clock.Clock) FileStore {
	m := NewLRUFileMap(size, clk)
	return &localFileStore{
		fileEntryFactory: NewCASFileEntryFactory(),
		fileMap:          m,
	}
}

// NewFileOp contructs a new FileOp object.
func (s *localFileStore) NewFileOp() FileOp {
	return NewLocalFileOp(s)
}
