package base

// FileStore manages files and their metadata. Actual operations are done through FileOp.
type FileStore interface {
	NewFileOp() FileOp
}

// localFileStore manages all agent files on local disk.
// Read/Write operation should access data in this order:
//   map load -> file lock -> verify not deleted -> map load/store -> file/metadata change -> file unlock
// Delete opereration should access data in this order:
//   map load -> file lock -> verify not deleted -> file/metadata change -> delete from map -> file unlock
type localFileStore struct {
	fileEntryFactory FileEntryFactory // Used for dependency injection.
	fileMap          FileMap          // Used for dependency injection.
}

// NewLocalFileStore initializes and returns a new FileStore. It allows dependency injection.
func NewLocalFileStore() (FileStore, error) {
	return &localFileStore{
		fileEntryFactory: NewLocalFileEntryFactory(),
		fileMap:          NewSimpleFileMap(),
	}, nil
}

// NewCASFileStore initializes and returns a new Content-Addressable FileStore.
// It uses the first few bytes of file digest (which is also used as file name) as shard ID.
// For every byte, one more level of directories will be created.
func NewCASFileStore() (FileStore, error) {
	return &localFileStore{
		fileEntryFactory: NewCASFileEntryFactory(),
		fileMap:          NewSimpleFileMap(),
	}, nil
}

// NewLRUFileStore initializes and returns a new LRU FileStore.
// When size exceeds limit, the least recently accessed entry will be removed.
func NewLRUFileStore(size int) (FileStore, error) {
	fm, err := NewLRUFileMap(size)
	if err != nil {
		return nil, err
	}
	return &localFileStore{
		fileEntryFactory: NewLocalFileEntryFactory(),
		fileMap:          fm,
	}, nil
}

// NewFileOp contructs a new FileOp object.
func (s *localFileStore) NewFileOp() FileOp {
	return NewLocalFileOp(s)
}
