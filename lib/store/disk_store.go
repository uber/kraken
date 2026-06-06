package store

import (
	"container/list"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/uber/kraken/lib/store/base"
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
	// TODO - pivot toward "complete" and "incomplete" blobs and pass config to support returning
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

// Get returns a committed blob. If the blob is not found, [os.ErrNotExists] is returned.
// The blob cannot be evicted before the client calls Close() on it.
func (s *DiskStore) Get(key string) (FileReader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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

// Stat returns [os.FileInfo] about the blob. Returns [os.ErrNotExists] if the blob is not found.
func (s *DiskStore) Stat(key string) (os.FileInfo, error) {
	s.mu.RLock()
	// We **could** avoid locking the mutex by just statting the file directly. However, the current implementation
	// prefers mutex contention over extra disk usage, as origin is bottlenecked by disk IO.
	_, okBlobs := s.blobs[key]
	_, okUnevictable := s.unevictableBlobs[key]
	s.mu.RUnlock()
	if !okBlobs && !okUnevictable {
		return nil, os.ErrNotExist
	}

	committed := true
	blobPath := s.blobPath(key, committed)
	return os.Stat(blobPath)
}

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

// Delete removes a blob and its [metadata.Metadata] from the store.
// If the blob is not committed, its write is canceled and any reserved resources are released.
// Does NOT work on unevictable blobs and returns [base.ErrFilePersisted].
func (s *DiskStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.unevictableBlobs[key]; ok {
		return base.ErrFilePersisted
	}

	size, ok := s.uncommittedBlobs[key]
	if ok {
		return s.cancelWrite(key, size)
	}

	node, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}

	committed := true
	err := s.deleteFromDisk(key, committed)
	if err != nil {
		return fmt.Errorf("delete from disk: %w", err)
	}
	delete(s.blobs, key)
	s.evictQueue.Remove(node)
	s.releaseSpace(node.Value.(el).size)
	return nil
}

func (s *DiskStore) cancelWrite(key string, size uint64) error {
	committed := false
	err := s.deleteFromDisk(key, committed)
	if err != nil {
		return fmt.Errorf("delete from disk: %w", err)
	}
	delete(s.uncommittedBlobs, key)
	s.releaseSpace(size)
	return nil
}

// List returns the keys of all committed blobs.
func (s *DiskStore) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]string, len(s.blobs)+len(s.unevictableBlobs))
	i := 0
	for key := range s.blobs {
		res[i] = key
		i++
	}
	for key := range s.unevictableBlobs {
		res[i] = key
		i++
	}
	return res
}

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
