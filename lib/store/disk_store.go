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
	_incompleteSubDir     = "incomplete"
	_completeSubDir       = "complete"
	_blobFileName         = "data"
)

// DiskStore is a key-value, persistent, thread-safe, LRU store for blobs and their [metadata.Metadata].
//
//   - Supports pagination of blobs during reading/writing, such that blobs don't need to be fully loaded into memory.
//
//   - New blobs are considered 'incomplete', which unlists them from LRU eviction. Read APIs may filter out incomplete blobs.
//
//   - All APIs are thread-safe. Parallel access to a single file is allowed but clients must ensure they don't intervene with one another.
//
//   - Supports (un-)marking blobs as non-evictable (may be needed when that data must be written back to remote storage).
//
//   - Crash-resistant - all state is restored upon restart (LRU order is approximated through file `ctime`).
//
//   - Uses directory sharding to speed up disk performance.
type DiskStore struct {
	capacity uint64
	size     uint64 // includes both used and reserved space
	dir      string
	// synchronizes mem state access and syscalls to the fs in the APIs (opening, moving files, etc.)
	mu sync.RWMutex // TODO - evaluate whether the read-to-write ratio is more appropriate for a [sync.Mutex] instead.
	// complete, evictable blobs.
	blobs map[string]*list.Element // value of [list.Element] is [el].
	// Back is most recently used, front is the next to evict.
	evictQueue      *list.List
	incompleteBlobs map[string]uint64
	// complete blobs that cannot be evicted.
	unevictableBlobs map[string]uint64
}

type el struct {
	key  string
	size uint64
}

// NewDiskStore creates a [DiskStore].
func NewDiskStore(capacityBytes uint64, rootDir string) (*DiskStore, error) {
	// TODO - create a Config struct.
	// TODO - recover persisted state in case of crash.
	return &DiskStore{
		dir:              rootDir,
		capacity:         capacityBytes,
		blobs:            make(map[string]*list.Element),
		evictQueue:       list.New(),
		incompleteBlobs:  make(map[string]uint64),
		unevictableBlobs: make(map[string]uint64),
		size:             0,
	}, nil
}

// Open returns an FD to a file in the store. [os.ErrNotExists] is returned on missing entry.
// The blob cannot be evicted before the client calls Close() on it.
func (s *DiskStore) Open(key string, ignoreIncomplete bool) (FileReadWriter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	unevictableBlobSize, okUnevictable := s.unevictableBlobs[key]
	if okUnevictable {
		complete := true
		return s.open(key, unevictableBlobSize, complete)
	}

	incompleteBlobSize, okIncomplete := s.incompleteBlobs[key]
	if okIncomplete && !ignoreIncomplete {
		complete := false
		return s.open(key, incompleteBlobSize, complete)
	}

	node, okBlob := s.blobs[key]
	if !okBlob {
		return nil, os.ErrNotExist
	}

	size := node.Value.(el).size
	s.evictQueue.MoveToBack(node)
	complete := true
	return s.open(key, size, complete)
}

func (s *DiskStore) open(key string, size uint64, complete bool) (FileReadWriter, error) {
	path := s.blobPath(key, complete)
	f, err := os.OpenFile(path, os.O_RDWR, _defaultFilePerm)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	return newReadWriter(f, size), nil
}

// Stat returns [os.FileInfo] about the blob. Returns [os.ErrNotExists] if the blob is not found.
func (s *DiskStore) Stat(key string, ignoreIncomplete bool) (os.FileInfo, error) {
	// We **could** avoid locking the mutex by just statting the file directly. However, the current implementation
	// prefers mutex contention over extra disk usage, as origin is bottlenecked by disk IO.
	s.mu.Lock()
	defer s.mu.Unlock()

	_, okBlob := s.blobs[key]
	_, okUnevictable := s.unevictableBlobs[key]
	_, okIncomplete := s.incompleteBlobs[key]

	if !okBlob && !okUnevictable && (ignoreIncomplete || !okIncomplete) {
		return nil, os.ErrNotExist
	}
	complete := okBlob || okUnevictable
	blobPath := s.blobPath(key, complete)
	return os.Stat(blobPath)
}

// Create adds a new, incomplete blob to the store. Incomplete entries cannot be automatically
// evicted. MarkComplete must be called once the blob is complete.
func (s *DiskStore) Create(key string, sizeBytes uint64) (FileReadWriter, error) {
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
	if _, ok := s.incompleteBlobs[key]; ok {
		return nil, errors.New("blob is already in store (it is incomplete)")
	}

	if err := s.reserveSpace(sizeBytes); err != nil {
		return nil, fmt.Errorf("reserve space: %w", err)
	}

	complete := false
	dirName := s.dirPath(key, complete)
	err := os.MkdirAll(dirName, _defaultFilePerm)
	if err != nil {
		s.releaseSpace(sizeBytes)
		return nil, fmt.Errorf("ensure dir: %w", err)
	}
	blobPath := s.blobPath(key, complete)
	flag := os.O_RDWR | os.O_CREATE | os.O_EXCL
	f, err := os.OpenFile(blobPath, flag, _defaultFilePerm)
	if err != nil {
		s.releaseSpace(sizeBytes)
		return nil, fmt.Errorf("open file: %w", err)
	}

	s.incompleteBlobs[key] = sizeBytes

	return newReadWriter(f, sizeBytes), nil
}

func (s *DiskStore) reserveSpace(space uint64) error {
	// TODO - emit latency to reserve space for a blob.
	for s.size+space > s.capacity {
		if s.evictQueue.Len() == 0 {
			return errors.New("cannot evict enough, the unevictable/incomplete blobs are using up all the space")
		}

		toEvictNode := s.evictQueue.Front()
		toEvictEl := toEvictNode.Value.(el)

		complete := true
		err := s.deleteFromDisk(toEvictEl.key, complete)
		if err != nil {
			// TODO - consider whether we want to fail-open by doing `continue` here.
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
func (s *DiskStore) deleteFromDisk(key string, complete bool) error {
	dir := s.dirPath(key, complete)
	return os.RemoveAll(dir)
}

// MarkComplete marks a blob as fully written. It enlists the blob for LRU eviction (unless ForbidEviction has been called).
// Additionally, read APIs may optionally filter out incomplete blobs.
func (s *DiskStore) MarkComplete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	size, ok := s.incompleteBlobs[key]
	if !ok {
		return fmt.Errorf("blob is not in incomplete state")
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

	delete(s.incompleteBlobs, key)
	// TODO - handle unevictable blobs correctly (they must transition to s.unevictableBlobs, not s.blobs and s.evictQueue)
	node := s.evictQueue.PushBack(el{key: key, size: size})
	s.blobs[key] = node
	return nil
}

// Delete removes a blob and its [metadata.Metadata] from the store. Works on incomplete blobs.
// Does NOT work on unevictable blobs and returns [base.ErrFilePersisted].
func (s *DiskStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.unevictableBlobs[key]; ok {
		return base.ErrFilePersisted
	}

	size, ok := s.incompleteBlobs[key]
	if ok {
		return s.cancelWrite(key, size)
	}

	node, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}

	complete := true
	err := s.deleteFromDisk(key, complete)
	if err != nil {
		return fmt.Errorf("delete from disk: %w", err)
	}
	delete(s.blobs, key)
	s.evictQueue.Remove(node)
	s.releaseSpace(node.Value.(el).size)
	return nil
}

func (s *DiskStore) cancelWrite(key string, size uint64) error {
	complete := false
	err := s.deleteFromDisk(key, complete)
	if err != nil {
		return fmt.Errorf("delete from disk: %w", err)
	}
	delete(s.incompleteBlobs, key)
	s.releaseSpace(size)
	return nil
}

// List returns the blobs' keys.
func (s *DiskStore) List(ignoreIncomplete bool) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	l := len(s.blobs) + len(s.unevictableBlobs)
	if !ignoreIncomplete {
		l += len(s.incompleteBlobs)
	}
	res := make([]string, l)
	i := 0
	for key := range s.blobs {
		res[i] = key
		i++
	}
	for key := range s.unevictableBlobs {
		res[i] = key
		i++
	}
	if !ignoreIncomplete {
		for key := range s.incompleteBlobs {
			res[i] = key
			i++
		}
	}
	return res
}

// ForbidEviction unlists a blob from LRU eviction.
// Needed when e.g. blobs must be written back to GCS/S3 and eviction before that is unacceptable.
func (s *DiskStore) ForbidEviction(key string) error { return errors.New("not implemented") }

// AllowEviction removes the effect of ForbidEviction for a blob.
func (s *DiskStore) AllowEviction(key string) error { return errors.New("not implemented") }

// WriteMetadata atomically stores `md` on disk. Can be called on both complete and incomplete blobs.
func (s *DiskStore) SetMetadata(key string, md metadata.Metadata) error {
	return errors.New("not implemented")
}

// GetMetadata populates `md` if the metadata is present. Returns [os.ErrNotExists] if key is not in store.
func (s *DiskStore) GetMetadata(key string, md metadata.Metadata, ignoreIncomplete bool) error {
	return errors.New("not implemented")
}

// DeleteMetadata removes the respective metadata, if present.
func (s *DiskStore) DeleteMetadata(key string, md metadata.Metadata) error {
	return errors.New("not implemented")
}

func (s *DiskStore) blobPath(key string, complete bool) string {
	dirName := s.dirPath(key, complete)
	return filepath.Join(dirName, _blobFileName)
}

func (s *DiskStore) dirPath(key string, complete bool) string {
	// TODO - allow config to specify whether to shard or not. Allow no sharding, so we can replace [SimpleStore].
	subDirName := _incompleteSubDir
	if complete {
		subDirName = _completeSubDir
	}
	dirPath := filepath.Join(s.dir, subDirName)
	for i := 0; i < int(_defaultShardIDLength) && i < len(key)/2; i++ {
		// (1 byte = 2 char of file name assumming file name is in HEX)
		dirName := key[i*2 : i*2+2]
		dirPath = filepath.Join(dirPath, dirName)
	}

	return filepath.Join(dirPath, key)
}
