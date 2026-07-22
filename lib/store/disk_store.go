package store

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/log"
	"go.uber.org/zap"
)

// the set of blobs that the store's APIs can operate on.
type blobScope int

// flags to scope [diskStore]'s APIs to a subset of blobs.
const (
	blobScopeAny blobScope = iota
	blobScopeComplete
	blobScopeIncomplete
)

const (
	_completeBlob           = true
	_incompleteBlob         = false
	_defaultFilePerm        = 0775
	_evictionBannedFileName = "_eviction_banned"
	_blobSizeFileName       = "_size"
)

// diskStore implements the APIs of [DiskStore]. [diskStore]'s APIs expose the [blobScope] arg,
// while [DiskStore]'s APIs omit that arg (cleaner interface) and instead expose other APIs to scope the whole store.
//
// Check [DiskStore]'s comments for details on functionality.
type diskStore struct {
	capacity   uint64
	size       uint64           // includes both used and reserved space.
	blobs      map[string]*blob // TODO - consider whether it's better to use struct instead of pointer to reduce GC stress.
	evictQueue *list.List       // Back is most recently used, front is the next to evict.
	// synchronizes mem state access and syscalls to the fs in the APIs (opening, moving files, etc.)
	mu sync.RWMutex // TODO - evaluate whether the read-to-write ratio is more appropriate for a [sync.Mutex] instead.
	// If enabled, incomplete blobs are rebooted in the store upon restart (or after a crash), allowing users to continue using the blob.
	// If disabled, incomplete blobs are discarded (usually done to prevent leaks).
	rebootIncompleteBlobs bool
	log                   *zap.SugaredLogger
	*pather
}

type blob struct {
	node           *list.Element // value of [list.Element] is [string].
	size           uint64
	complete       bool
	evictionBanned bool
}

func newDiskStore(capacityBytes uint64, rootDir string, rebootIncompleteBlobs bool) (*diskStore, error) {
	// TODO - create a Config struct.
	// TODO - consider how to support blob mutation, which might be needed by build-index for tag mutation.
	// TODO - move disk store files into their own directory and package.

	log := log.Default().With("module", "disk_store")
	ok, err := existsPersistedStore(rootDir)
	if err != nil {
		err = fmt.Errorf("could not check if previously-left persisted state exists on disk: %w", err)
		log.With("error", err).Error("Failed to initialize disk store")
		return nil, err
	}
	if !ok {
		log.Info("Initialized a new, empty DiskStore (did not find any previously persisted state to reboot for DiskStore)")
		return &diskStore{
			capacity:              capacityBytes,
			size:                  0,
			blobs:                 make(map[string]*blob),
			evictQueue:            list.New(),
			log:                   log,
			pather:                newPather(rootDir),
			rebootIncompleteBlobs: rebootIncompleteBlobs,
		}, nil
	}

	store, err := rebootPersistedStore(capacityBytes, rootDir, rebootIncompleteBlobs, log)
	if err != nil {
		err = fmt.Errorf("reboot persisted state into memory: %w", err)
		log.With("error", err).Error("Failed to initialize disk store")
		return nil, err
	}
	log.With("num_blobs", len(store.blobs)).Info("Successfully rebooted DiskStore's previously left state on disk")
	return store, nil
}

func (s *diskStore) open(key string, scope blobScope) (FileReadWriter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return nil, err
	}

	if b.node != nil {
		s.evictQueue.MoveToBack(b.node)
	}
	path := s.blobPath(key, b.complete)
	f, err := os.OpenFile(path, os.O_RDWR, _defaultFilePerm)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	return newReadWriter(f), nil
}

func (s *diskStore) stat(key string, scope blobScope) (os.FileInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return nil, err
	}
	blobPath := s.blobPath(key, b.complete)
	return os.Stat(blobPath)
}

func (s *diskStore) create(key string, sizeBytes uint64) (FileReadWriter, error) {
	// TODO - we might want some TTI on uploads to the store, after which we cancel the upload, e.g. 1min without the client uploading more data.
	s.mu.Lock()
	defer s.mu.Unlock()

	if b, ok := s.blobs[key]; ok {
		// TODO - consider whether we need public errors for these cases.
		if b.complete {
			return nil, errors.New("blob is already in store")
		} else {
			return nil, errors.New("blob is already in store (it is incomplete)")
		}
	}

	if err := s.reserveSpace(sizeBytes); err != nil {
		return nil, fmt.Errorf("reserve space: %w", err)
	}

	dirName := s.dirPath(key, _incompleteBlob)
	err := os.MkdirAll(dirName, _defaultFilePerm)
	if err != nil {
		s.releaseSpace(sizeBytes)
		return nil, fmt.Errorf("ensure dir: %w", err)
	}
	blobPath := s.blobPath(key, _incompleteBlob)
	flag := os.O_RDWR | os.O_CREATE | os.O_EXCL
	f, err := os.OpenFile(blobPath, flag, _defaultFilePerm)
	if err != nil {
		s.releaseSpace(sizeBytes)
		return nil, fmt.Errorf("open file: %w", err)
	}

	if s.rebootIncompleteBlobs {
		err = s.persistBlobSize(key, sizeBytes)
		if err != nil {
			// Fail-open: the blob will be discarded upon reboot if incomplete.
			s.log.With("error", err).Error("Could not persist client-provided blob size on disk")
		}
	}

	s.blobs[key] = &blob{
		size:           sizeBytes,
		node:           nil,
		complete:       false,
		evictionBanned: false,
	}

	return newReadWriter(f), nil
}

func (s *diskStore) persistBlobSize(key string, sizeBytes uint64) error {
	blobSizeFilePath := s.sidecarFilePath(key, _incompleteBlob, _blobSizeFileName)
	blobSizeF, err := os.OpenFile(blobSizeFilePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, _defaultFilePerm)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	_, err = blobSizeF.Write([]byte(strconv.Itoa(int(sizeBytes))))
	if err != nil {
		return fmt.Errorf("write to size file: %w", err)
	}
	closers.Close(blobSizeF)
	return nil
}

func (s *diskStore) reserveSpace(space uint64) error {
	// TODO - benchmark and consider whether async eviction makes more sense.
	// TODO - emit latency to reserve space for a blob.
	for s.size+space > s.capacity {
		if s.evictQueue.Len() == 0 {
			return errors.New("cannot evict enough, the unevictable/incomplete blobs are using up all the space")
		}

		toEvictNode := s.evictQueue.Front()
		toEvictKey := toEvictNode.Value.(string)

		err := s.deleteFromDisk(toEvictKey, _completeBlob)
		if err != nil {
			// TODO - consider whether we want to fail-open by doing `continue` here.
			return fmt.Errorf("delete from disk: %w", err)
		}
		s.evictQueue.Remove(toEvictNode)
		size := s.blobs[toEvictKey].size
		s.releaseSpace(size)
		delete(s.blobs, toEvictKey)
	}

	s.size += space
	return nil
}

func (s *diskStore) releaseSpace(space uint64) {
	if space > s.size {
		s.log.Error("Invariant violation - DiskStore wants to release more disk space than actually reserved. Failing open by releasing all reserved space.")
		s.size = 0
		return
	}
	s.size -= space
}

// Fully deletes the disk state of a blob, including metadata. Works on any blob.
func (s *diskStore) deleteFromDisk(key string, complete bool) error {
	dir := s.dirPath(key, complete)
	return os.RemoveAll(dir)
}

func (s *diskStore) markComplete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}
	if b.complete {
		// no-op
		return nil
	}

	oldPathDir := s.dirPath(key, _incompleteBlob)
	newPathDir := s.dirPath(key, _completeBlob)
	err := os.MkdirAll(filepath.Dir(newPathDir), _defaultFilePerm)
	if err != nil {
		return fmt.Errorf("mkdirall: %w", err)
	}
	// TODO - make sure that un-movable metadata is deleted after move
	err = os.Rename(oldPathDir, newPathDir)
	if err != nil {
		return fmt.Errorf("move dir: %w", err)
	}
	b.complete = true
	if !b.evictionBanned {
		node := s.evictQueue.PushBack(key)
		b.node = node
	}
	return nil
}

func (s *diskStore) checkDiskIfUnevictable(key string, complete bool) (bool, error) {
	flagBlobPath := s.sidecarFilePath(key, complete, _evictionBannedFileName)
	unevictable, err := exists(flagBlobPath)
	if err != nil {
		return false, err
	}
	return unevictable, nil
}

func (s *diskStore) delete(key string, scope blobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return err
	}
	err := s.deleteFromDisk(key, b.complete)
	if err != nil {
		return fmt.Errorf("delete from disk: %w", err)
	}
	if b.node != nil {
		s.evictQueue.Remove(b.node)
		b.node = nil
	}
	delete(s.blobs, key)
	s.releaseSpace(b.size)

	return nil
}

func (s *diskStore) list(scope blobScope) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]string, 0, len(s.blobs))
	for key, b := range s.blobs {
		if err := isOutOfScope(b, scope); err != nil {
			continue
		}
		res = append(res, key)
	}
	return res
}

func (s *diskStore) banEviction(key string, scope blobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return err
	}
	if b.evictionBanned {
		// no-op
		return nil
	}

	flagBlobPath := s.sidecarFilePath(key, b.complete, _evictionBannedFileName)
	// We persist the ban as a flag file on disk for crash-resilience.
	f, err := os.OpenFile(flagBlobPath, os.O_RDONLY|os.O_CREATE, _defaultFilePerm)
	if err != nil {
		return fmt.Errorf("create file that flags eviction as banned: %w", err)
	}
	closers.Close(f)

	b.evictionBanned = true
	if b.complete {
		s.evictQueue.Remove(b.node)
		b.node = nil
	}
	return nil
}

func (s *diskStore) unbanEviction(key string, scope blobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return err
	}
	if !b.evictionBanned {
		// no-op
		return nil
	}

	flagBlobPath := s.sidecarFilePath(key, b.complete, _evictionBannedFileName)
	err := os.Remove(flagBlobPath)
	if err != nil {
		return fmt.Errorf("remove file that flags eviction as banned: %w", err)
	}

	b.evictionBanned = false
	if b.complete {
		node := s.evictQueue.PushBack(key)
		b.node = node
	}
	return nil
}

func (s *diskStore) setMetadata(key string, md metadata.Metadata, scope blobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return err
	}

	mdData, err := md.Serialize()
	if err != nil {
		return fmt.Errorf("serialize metadata: %w", err)
	}
	mdFilePath := s.sidecarFilePath(key, b.complete, md.GetSuffix())
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

func (s *diskStore) getMetadata(key string, md metadata.Metadata, scope blobScope) (ok bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	b, ok := s.blobs[key]
	if !ok {
		return false, os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return false, err
	}

	mdFilePath := s.sidecarFilePath(key, b.complete, md.GetSuffix())
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

func (s *diskStore) deleteMetadata(key string, md metadata.Metadata, scope blobScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.blobs[key]
	if !ok {
		return os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return err
	}
	mdFilePath := s.sidecarFilePath(key, b.complete, md.GetSuffix())
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
func (s *diskStore) evictionOrder() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	evictionOrder := make([]string, 0)
	for curr := s.evictQueue.Front(); curr != nil; curr = curr.Next() {
		currKey := curr.Value.(string)
		evictionOrder = append(evictionOrder, currKey)
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

func isOutOfScope(b *blob, scope blobScope) error {
	if (b.complete && scope == blobScopeIncomplete) || (!b.complete && scope == blobScopeComplete) {
		return ErrOutOfScope
	}
	return nil
}
