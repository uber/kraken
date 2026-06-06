package store

import (
	"container/list"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/uber/kraken/lib/store/metadata"
)

const (
	_defaultFilePerm = 0775
	// _defaultShardIDLength is the number of bytes of file digest to be used for shard ID.
	// For every byte (2 HEX char), one more level of directories will be created.
	_defaultShardIDLength = 2
	_uncommittedSubDir    = "uncommitted"
	_committedSubDir      = "committed"
	_blobFileName         = "data"
)

// DiskStore is a content-addressable, persistent, fully thread-safe, LRU store for blobs and their [metadata.Metadata].
//
//   - Supports pagination of blobs during reading, such that they don't need to be fully loaded into memory.
//
//   - Supports (un-)marking blobs as non-evictable (may be needed when that data must be written back to remote storage).
//
//   - Supports atomic writes through a Commit function to ensure state is never corrupted on errors/crashes.
//
//   - Crash-resistant - all state is restored upon restart (LRU order is approximated through file `ctime`).
//
//   - Uses directory sharding to speed up disk performance.
type DiskStore struct {
	capacity uint64
	size     uint64 // includes both used and reserved space
	dir      string
	mu       sync.RWMutex // TODO - evaluate whether the read-to-write ratio is more appropriate for a [sync.Mutex] instead.
	// Committed, evictable blobs.
	blobs map[string]*list.Element // value of [list.Element] is [el].
	// Back is most recently used, front is the next to evict.
	evictQueue       *list.List
	uncommittedBlobs map[string]uint64
	// committed blobs that cannot be evicted.
	unevictableBlobs map[string]uint64
}

type el struct {
	key  string
	size uint64
}

// NewDiskStore creates a [DiskStore].
func NewDiskStore(capacityBytes uint64, rootDir string) (*DiskStore, error) {
	// TODO - recover persisted state in case of crash.
	return &DiskStore{
		dir:              rootDir,
		capacity:         capacityBytes,
		blobs:            make(map[string]*list.Element),
		evictQueue:       list.New(),
		uncommittedBlobs: make(map[string]uint64),
		unevictableBlobs: make(map[string]uint64),
		size:             0,
	}, nil
}

// Returns os.ErrNotExist if the blob is not present.
// TODO - decide what happens if the blob is being writtem atm - should return a special error `InProgress` or keep returning `ErrNotExists`? - probably go for the simpler thing and return ErrNotExists until Commit is done. Atomicity should
// be encapsulated as much as possible within this package. From the client's POV blobs are either in or outside the store. Consider whether we want to make an exception to that rule during writes for debugging purposes (makes sense there, as the client should know what state of the atomic write they are in).
// and return ErrNotExists.
// TODO - decide what is done when a blob is evicted while io.Reader is held by the client (check what's done right now and evaluate if we should mimic). - probably reuse linux semantics here (while the fd is kept, client can continue reading)
func (s *DiskStore) Get(key string) (FileReader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	node, okBlobs := s.blobs[key]
	unevictableBlobSize, okUnevictable := s.unevictableBlobs[key]
	if !okBlobs && !okUnevictable {
		return nil, os.ErrNotExist
	}

	if okUnevictable {
		return s.getBlob(key, unevictableBlobSize)
	}

	size := node.Value.(el).size
	s.evictQueue.MoveToBack(node)
	return s.getBlob(key, size)
}

func (s *DiskStore) getBlob(key string, size uint64) (FileReader, error) {
	committed := true
	path := s.blobPath(key, committed)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	return newReadWriter(f, size), nil
}

// Stat returns [os.FileInfo] about the blob.
func (s *DiskStore) Stat(key string) (os.FileInfo, error) { return nil, errors.New("not implemented") }

// StartWrite initializes an uncommitted write to the store for a new blob.
// The client MUST call CommitWrite once the blob is fully written to enable reading it.
func (s *DiskStore) StartWrite(key string, sizeBytes uint64) (FileReadWriter, error) {
	// TODO - we might want some TTI on uploads to the store, after which we cancel the upload, e.g. 1min without the client uploading more data.
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.blobs[key]; ok {
		// TODO - consider whether we need public errors for these cases.
		return nil, errors.New("blob is already in store")
	}
	if _, ok := s.unevictableBlobs[key]; ok {
		return nil, errors.New("blob is already in store")
	}
	if _, ok := s.uncommittedBlobs[key]; ok {
		return nil, errors.New("blob write already in progress")
	}

	if err := s.reserveSpace(sizeBytes); err != nil {
		return nil, fmt.Errorf("reserve space: %w", err)
	}

	committed := false
	dirName := s.dirPath(key, committed)
	err := os.MkdirAll(dirName, _defaultFilePerm)
	if err != nil {
		s.releaseSpace(sizeBytes)
		return nil, fmt.Errorf("ensure dir: %w", err)
	}
	blobPath := s.blobPath(key, committed)
	flag := os.O_RDWR | os.O_CREATE | os.O_EXCL
	f, err := os.OpenFile(blobPath, flag, _defaultFilePerm)
	if err != nil {
		s.releaseSpace(sizeBytes)
		return nil, fmt.Errorf("open file: %w", err)
	}

	s.uncommittedBlobs[key] = sizeBytes

	return newReadWriter(f, sizeBytes), nil
}

func (s *DiskStore) reserveSpace(space uint64) error {
	// TODO - emit latency to reserve space for a blob.
	for s.size+space > s.capacity {
		if s.evictQueue.Len() == 0 {
			return errors.New("cannot evict enough, the unevictable/uncommitted blobs are using up all the space")
		}

		toEvictNode := s.evictQueue.Front()
		toEvictEl := toEvictNode.Value.(el)

		committed := true
		err := s.deleteFromDisk(toEvictEl.key, committed)
		if err != nil {
			return fmt.Errorf("delete from disk: %w", err)
		}
		s.evictQueue.Remove(toEvictNode)
		delete(s.blobs, toEvictEl.key)
		s.releaseSpace(toEvictEl.size)
	}

	s.size += space
	return nil
}

func (s *DiskStore) releaseSpace(space uint64) {
	// TODO - if space > s.size, emit an error log for an invariant violation
	s.size -= space
}

// fully deletes the state of a blob, including metadata. Works both on persisted and non-persisted blobs.
func (s *DiskStore) deleteFromDisk(key string, committed bool) error {
	dir := s.dirPath(key, committed)
	return os.RemoveAll(dir)
}

// CommitWrite must be called once a blob is fully written to make it visible to clients for reading.
func (s *DiskStore) CommitWrite(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	size, ok := s.uncommittedBlobs[key]
	if !ok {
		return fmt.Errorf("blob is not in uncommitted state")
	}

	oldPathDir := s.dirPath(key, false)
	newPathDir := s.dirPath(key, true)
	err := os.MkdirAll(filepath.Dir(newPathDir), _defaultFilePerm)
	if err != nil {
		return fmt.Errorf("mkdirall: %w", err)
	}
	err = os.Rename(oldPathDir, newPathDir) // atomic
	if err != nil {
		return fmt.Errorf("move dir: %w", err)
	}

	delete(s.uncommittedBlobs, key)
	// TODO - handle unevictable blobs correctly (they must transition to s.unevictableBlobs, not s.blobs and s.evictQueue)
	node := s.evictQueue.PushBack(el{key: key, size: size})
	s.blobs[key] = node
	return nil
}

// CancelWrite should be called to free resources if an upload to the store has been started but will not be finished (e.g. due to an error).
func (s *DiskStore) CancelWrite(key string) error { return errors.New("not implemented") } // TODO - evaulate whether we can remove this API in favor of using Delete for uncommitted blobs.

// Delete removes a blob and its [metadata.Metadata] from the store. Does NOT work on unevictable blobs and returns [base.ErrFilePersisted].
// CancelWrite should be used on uncommitted blobs. // TODO - reevaluate this decision.
func (s *DiskStore) Delete(key string) error { return errors.New("not implemented") }

// List returns the keys of all committed blobs stored in the store.
func (s *DiskStore) List() ([]string, error) { return nil, errors.New("not implemented") }

// ForbidEviction stops a blob from being evicted by the LRU until [AllowEviction] is called. Needed when blobs must be written back to GCS/S3/etc.
func (s *DiskStore) ForbidEviction(key string) error { return errors.New("not implemented") }

// AllowEviction removes the effect of ForbidEviction for a blob.
func (s *DiskStore) AllowEviction(key string) error { return errors.New("not implemented") }

// WriteMetadata atomically stores `md` on disk. Can be called both before and after a blob has been committed.
func (s *DiskStore) SetMetadata(key string, md metadata.Metadata) error {
	return errors.New("not implemented")
}

// GetMetadata populates `md` if the metadata is present. Can be called both before and after a blob has been committed.
func (s *DiskStore) GetMetadata(key string, md metadata.Metadata) error {
	return errors.New("not implemented")
}

// DeleteMetadata removes the respective metadata, if present.
func (s *DiskStore) DeleteMetadata(key string, md metadata.Metadata) error {
	return errors.New("not implemented")
}

func (s *DiskStore) blobPath(key string, committed bool) string {
	dirName := s.dirPath(key, committed)
	return filepath.Join(dirName, _blobFileName)
}

func (s *DiskStore) dirPath(key string, committed bool) string {
	subDirName := _uncommittedSubDir
	if committed {
		subDirName = _committedSubDir
	}
	dirPath := filepath.Join(s.dir, subDirName)
	for i := 0; i < int(_defaultShardIDLength) && i < len(key)/2; i++ {
		// (1 byte = 2 char of file name assumming file name is in HEX)
		dirName := key[i*2 : i*2+2]
		dirPath = filepath.Join(dirPath, dirName)
	}

	return filepath.Join(dirPath, key)
}
