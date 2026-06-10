package store

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/log"
	"go.uber.org/zap"
)

const (
	// _defaultShardIDLength is the number of bytes of file digest to be used for shard ID.
	// For every byte (2 HEX char), one more level of directories will be created.
	_defaultFilePerm        = 0775
	_evictionBannedFileName = "_eviction_banned"
	// TODO - change this not to be hardcoded when configurable sharding is implemented
	_numShards = 2
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
//   - Crash-resistant - all state is restored upon restart (LRU order is approximated through file `mtime`).
//
//   - Uses directory sharding to speed up disk performance.
type DiskStore struct {
	*pather
	capacity uint64
	size     uint64 // includes both used and reserved space
	// synchronizes mem state access and syscalls to the fs in the APIs (opening, moving files, etc.)
	mu sync.RWMutex // TODO - evaluate whether the read-to-write ratio is more appropriate for a [sync.Mutex] instead.
	// complete, evictable blobs.
	blobs map[string]*list.Element // value of [list.Element] is [el].
	// Back is most recently used, front is the next to evict.
	evictQueue *list.List
	// incomplete blobs that may or may not be evictable.
	incompleteBlobs map[string]uint64
	// complete blobs that cannot be evicted.
	unevictableBlobs map[string]uint64
	log              *zap.SugaredLogger
}

type el struct {
	key  string
	size uint64
}

// NewDiskStore creates a [*DiskStore].
// incomplete blobs are deleted
// approximates lru eviction order by file `mtime`
func NewDiskStore(capacityBytes uint64, rootDir string) (*DiskStore, error) {
	// TODO - create a Config struct.
	// TODO - consider how to support blob mutation, which might be needed by build-index for tag mutation.
	// TODO - move disk store files into their own directory and package.

	log := log.Default().With("module", "disk_store")
	ok, err := existsPersistedState(rootDir)
	if err != nil {
		err = fmt.Errorf("could not check if previously-left persisted state exists on disk: %w", err)
		log.With("error", err).Error("Failed to initialize disk store")
		return nil, err
	}
	if !ok {
		log.Info("Did not find any previously persisted state to reboot for DiskStore - initializing a new, empty DiskStore")
		return &DiskStore{
			capacity:         capacityBytes,
			blobs:            make(map[string]*list.Element),
			evictQueue:       list.New(),
			incompleteBlobs:  make(map[string]uint64),
			unevictableBlobs: make(map[string]uint64),
			size:             0,
			log:              log,
			pather:           newPather(rootDir),
		}, nil
	}

	store, err := rebootPersistedStateAfterCrash(capacityBytes, rootDir, log)
	if err != nil {
		err = fmt.Errorf("reboot persisted state into memory: %w", err)
		log.With("error", err).Error("Failed to initialize disk store")
		return nil, err
	}
	log.With("num_blobs", len(store.blobs)+len(store.unevictableBlobs)).Info("Successfully initialized disk store")
	return store, nil
}

// Open returns an FD to a file in the store. [os.ErrNotExists] is returned on missing entry.
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

// Create adds a new, incomplete blob to the store and reserves space for it.
// Incomplete entries cannot be automatically evicted. MarkComplete must be called once the blob is complete.
// DiskStore does not ever check/use the real size of the blob and only uses `sizeBytes` for its eviction logic.
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

// fully deletes the disk state of a blob, including metadata. Works on any blob.
func (s *DiskStore) deleteFromDisk(key string, complete bool) error {
	dir := s.dirPath(key, complete)
	return os.RemoveAll(dir)
}

// MarkComplete marks a blob as fully written. It enlists the blob for LRU eviction (unless BanEviction has been called).
// Additionally, read APIs may optionally filter out incomplete blobs.
func (s *DiskStore) MarkComplete(key string) error {
	// TODO - check if we can derive when a blob is considered complete (e.g. when client calls Close on file (although that depends on the
	// assumption that Close means the file is complete which may not be true if the client that created the file expects another client to continue mutating it)).
	s.mu.Lock()
	defer s.mu.Unlock()

	size, okIncomplete := s.incompleteBlobs[key]
	_, okBlob := s.blobs[key]
	_, okUnevictable := s.unevictableBlobs[key]
	if !okIncomplete && !okBlob && !okUnevictable {
		return os.ErrNotExist
	}

	if !okIncomplete {
		// no-op
		return nil
	}

	oldPathDir := s.dirPath(key, false)
	newPathDir := s.dirPath(key, true)
	complete := false
	isUnevictable, err := s.checkDiskIfUnevictable(key, complete)
	if err != nil {
		return fmt.Errorf("check if blob is evictable or not: %w", err)
	}
	err = os.MkdirAll(filepath.Dir(newPathDir), _defaultFilePerm)
	if err != nil {
		return fmt.Errorf("mkdirall: %w", err)
	}
	err = os.Rename(oldPathDir, newPathDir) // atomic
	if err != nil {
		return fmt.Errorf("move dir: %w", err)
	}
	delete(s.incompleteBlobs, key)
	if isUnevictable {
		s.unevictableBlobs[key] = size
		return nil
	}

	node := s.evictQueue.PushBack(el{key: key, size: size})
	s.blobs[key] = node
	return nil
}

func (s *DiskStore) checkDiskIfUnevictable(key string, complete bool) (bool, error) {
	flagBlobPath := s.sidecarFilePath(key, complete, _evictionBannedFileName)
	unevictable, err := exists(flagBlobPath)
	if err != nil {
		return false, err
	}
	return unevictable, nil
}

// Delete removes a blob and its [metadata.Metadata] from the store. Works on any blob.
func (s *DiskStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if size, ok := s.unevictableBlobs[key]; ok {
		complete := true
		err := s.deleteFromDisk(key, complete)
		if err != nil {
			return fmt.Errorf("delete from disk: %w", err)
		}
		delete(s.unevictableBlobs, key)
		s.releaseSpace(size)
		return nil
	}
	if size, ok := s.incompleteBlobs[key]; ok {
		complete := false
		err := s.deleteFromDisk(key, complete)
		if err != nil {
			return fmt.Errorf("delete from disk: %w", err)
		}
		delete(s.incompleteBlobs, key)
		s.releaseSpace(size)
		return nil
	}
	if node, ok := s.blobs[key]; ok {
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

	return os.ErrNotExist
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

// BanEviction marks a blob as unevictable by LRU eviction. It is idempotent.
// Needed when e.g. blobs must be written back to GCS/S3 and eviction before that is unacceptable.
func (s *DiskStore) BanEviction(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, okBlob := s.blobs[key]
	_, okUnevictable := s.unevictableBlobs[key]
	_, okIncomplete := s.incompleteBlobs[key]

	if !okBlob && !okUnevictable && !okIncomplete {
		return os.ErrNotExist
	}
	if okUnevictable {
		// no-op
		return nil
	}

	complete := okBlob
	flagBlobPath := s.sidecarFilePath(key, complete, _evictionBannedFileName)
	// We persist the ban as a flag file on disk, such that after
	// a system crash, we can recover the ban.
	f, err := os.OpenFile(flagBlobPath, os.O_RDONLY|os.O_CREATE, _defaultFilePerm)
	if err != nil {
		return fmt.Errorf("create file that flags eviction as banned: %w", err)
	}
	closers.Close(f)

	if okIncomplete {
		return nil
	}

	nodeEl := node.Value.(el)
	delete(s.blobs, key)
	s.evictQueue.Remove(node)
	s.unevictableBlobs[key] = nodeEl.size
	return nil
}

// UnbanDeletion removes the effect of BanDeletion for a blob. It is idempotent.
func (s *DiskStore) UnbanEviction(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, okBlob := s.blobs[key]
	sizeUnevictable, okUnevictable := s.unevictableBlobs[key]
	_, okIncomplete := s.incompleteBlobs[key]

	if !okBlob && !okUnevictable && !okIncomplete {
		return os.ErrNotExist
	}

	if okBlob {
		// no-op
		return nil
	}

	complete := okUnevictable
	isUnevictable, err := s.checkDiskIfUnevictable(key, complete)
	if err != nil {
		return fmt.Errorf("check on disk if file is evictable: %w", err)
	}
	if !isUnevictable {
		// no-op
		if okUnevictable {
			// TODO - log invariant violation
		}
		return nil
	}
	flagBlobPath := s.sidecarFilePath(key, complete, _evictionBannedFileName)
	err = os.Remove(flagBlobPath)
	if err != nil {
		return fmt.Errorf("remove file that flags eviction as banned: %w", err)
	}
	if okIncomplete {
		return nil
	}

	delete(s.unevictableBlobs, key)
	node := s.evictQueue.PushBack(el{key: key, size: sizeUnevictable})
	s.blobs[key] = node
	return nil
}

// SetMetadata atomically sets the respective metadata for a blob. Works on any blob.
func (s *DiskStore) SetMetadata(key string, md metadata.Metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, okBlob := s.blobs[key]
	_, okUnevictable := s.unevictableBlobs[key]
	_, okIncomplete := s.incompleteBlobs[key]
	if !okBlob && !okUnevictable && !okIncomplete {
		return os.ErrNotExist
	}

	mdData, err := md.Serialize()
	if err != nil {
		return fmt.Errorf("serialize metadata: %w", err)
	}
	complete := !okIncomplete
	mdFilePath := s.sidecarFilePath(key, complete, md.GetSuffix())
	// We use a tmp file to ensure atomicity.
	tmpFilePath := mdFilePath + "-tmp"
	tmpFile, err := os.OpenFile(tmpFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, _defaultFilePerm)
	if err != nil {
		return fmt.Errorf("create tmp file for md: %w", err)
	}
	_, err = tmpFile.Write(mdData)
	if err != nil {
		return fmt.Errorf("write to tmp file: %w", err)
	}
	err = tmpFile.Close()
	if err != nil {
		return fmt.Errorf("close tmp file: %w", err)
	}
	err = os.Rename(tmpFile.Name(), mdFilePath)
	if err != nil {
		return fmt.Errorf("rename tmp file: %w", err)
	}
	return nil
}

// GetMetadata populates `md` if the metadata is present. Returns [os.ErrNotExists] if key is not in store.
func (s *DiskStore) GetMetadata(key string, md metadata.Metadata, ignoreIncomplete bool) (ok bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, okBlob := s.blobs[key]
	_, okUnevictable := s.unevictableBlobs[key]
	_, okIncomplete := s.incompleteBlobs[key]
	if !okBlob && !okUnevictable && (ignoreIncomplete || !okIncomplete) {
		return false, os.ErrNotExist
	}

	complete := !okIncomplete
	mdFilePath := s.sidecarFilePath(key, complete, md.GetSuffix())
	mdFile, err := os.OpenFile(mdFilePath, os.O_RDONLY, _defaultFilePerm)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	defer closers.Close(mdFile)
	data, err := io.ReadAll(mdFile)
	if err != nil {
		return false, fmt.Errorf("read from metadata file: %w", err)
	}
	err = md.Deserialize(data)
	if err != nil {
		return false, fmt.Errorf("deserialize into metadata: %w", err)
	}
	return true, nil
}

// DeleteMetadata removes any metadata of a blob with `md`'s suffix, if present.
func (s *DiskStore) DeleteMetadata(key string, md metadata.Metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, okBlob := s.blobs[key]
	_, okUnevictable := s.unevictableBlobs[key]
	_, okIncomplete := s.incompleteBlobs[key]
	if !okBlob && !okUnevictable && !okIncomplete {
		return os.ErrNotExist
	}
	complete := !okIncomplete
	mdFilePath := s.sidecarFilePath(key, complete, md.GetSuffix())
	err := os.Remove(mdFilePath)
	if errors.Is(err, os.ErrNotExist) {
		// no-op
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove metadata file: %w", err)
	}
	return nil
}

// used during testing
func (s *DiskStore) evictionOrder() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	evictionOrder := make([]string, 0)
	for curr := s.evictQueue.Front(); curr != nil; curr = curr.Next() {
		currEl := curr.Value.(el)
		evictionOrder = append(evictionOrder, currEl.key)
	}
	return evictionOrder
}

func exists(path string) (ok bool, err error) {
	_, err = os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("stat: %w", err)
}
