package disk

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/uber-go/tally"
	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/log"
	"go.uber.org/zap"
)

const (
	_completeBlob           = true
	_incompleteBlob         = false
	_defaultFilePerm        = 0775
	_evictionBannedFileName = "_eviction_banned"
	_blobSizeFileName       = "_size"
)

var _syncEvictionLatencyBuckets = tally.MustMakeExponentialDurationBuckets(100*time.Millisecond, 1.4, 15)

// store implements the APIs of [Store]. [store]'s APIs expose the [storelib.BlobScope] arg,
// while [Store]'s APIs omit that arg (cleaner interface) and instead expose other APIs to scope the whole store.
//
// Check [Store]'s comments for details on functionality.
type store struct {
	capacity   uint64
	size       uint64           // Includes both actively-used and not yet used, but reserved space.
	blobs      map[string]*blob // TODO - consider whether it's better to use struct instead of pointer to reduce GC stress.
	evictQueue *list.List       // Front is the next to evict.
	// Synchronizes 1) mem state and 2) disk state. Only acquired by public methods.
	mu      sync.RWMutex // TODO - evaluate whether the read-to-write ratio is more appropriate for a [sync.Mutex] instead.
	config  *Config
	log     *zap.SugaredLogger
	metrics tally.Scope
	*pather
}

type blob struct {
	node           *list.Element // Value of [list.Element] is [string].
	size           uint64
	complete       bool
	evictionBanned bool
}

func newStore(config *Config, metrics tally.Scope) (*store, error) {
	log := log.Default().With("module", "disk_store")
	ok, err := existsPersistedStore(config.RootDir)
	if err != nil {
		err = fmt.Errorf("could not check if previously-left persisted state exists on disk: %w", err)
		log.With("error", err).Error("Failed to initialize disk store")
		return nil, err
	}
	if !ok {
		log.Info("Initialized a new, empty Store (did not find any previously persisted state to reboot for Store)")
		store := &store{
			capacity:   config.CapacityBytes,
			size:       0,
			blobs:      make(map[string]*blob),
			evictQueue: list.New(),
			config:     config,
			pather:     newPather(config.RootDir, config.ShardLength),
			log:        log,
			metrics:    metrics,
		}

		store.emitUsageMetrics()
		return store, nil
	}

	store, err := rebootPersistedStore(config, log, metrics)
	if err != nil {
		err = fmt.Errorf("reboot persisted state into memory: %w", err)
		log.With("error", err).Error("Failed to initialize disk store")
		return nil, err
	}
	log.With("num_blobs", len(store.blobs)).Info("Successfully rebooted Store's previously left state on disk")

	store.emitUsageMetrics()
	return store, nil
}

func (s *store) Open(key string, scope storelib.BlobScope) (storelib.FileReadWriter, error) {
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
	return storelib.NewReadWriter(f), nil
}

func (s *store) Stat(key string, scope storelib.BlobScope) (os.FileInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	b, ok := s.blobs[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return nil, err
	}
	// TODO - consider whether this should update the access time of the blob.
	blobPath := s.blobPath(key, b.complete)
	return os.Stat(blobPath)
}

func (s *store) Create(key string, sizeBytes uint64) (storelib.FileReadWriter, error) {
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
	f, err := os.OpenFile(blobPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, _defaultFilePerm)
	if err != nil {
		s.releaseSpace(sizeBytes)
		return nil, fmt.Errorf("open file: %w", err)
	}

	if s.config.RebootIncompleteBlobs {
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

	s.emitUsageMetrics()
	return storelib.NewReadWriter(f), nil
}

func (s *store) persistBlobSize(key string, sizeBytes uint64) error {
	blobSizeFilePath := s.sidecarFilePath(key, _incompleteBlob, _blobSizeFileName)
	blobSizeF, err := os.OpenFile(blobSizeFilePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, _defaultFilePerm)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer closers.Close(blobSizeF)
	_, err = blobSizeF.Write([]byte(strconv.Itoa(int(sizeBytes))))
	if err != nil {
		return fmt.Errorf("write to size file: %w", err)
	}
	return nil
}

func (s *store) reserveSpace(space uint64) error {
	// TODO - benchmark and consider whether async eviction makes more sense.
	startTime := time.Now()
	for s.size+space > s.capacity {
		if s.evictQueue.Len() == 0 {
			s.log.With(
				"unevictable_bytes", s.size,
				"required_space", space,
				"capacity", s.capacity,
			).Error("Cannot evict enough data to free space for new entry to Store. The unevictable/incomplete blobs are using up all the space")
			return errors.New("cannot evict enough, the unevictable/incomplete blobs are using up all the space")
		}

		toEvictNode := s.evictQueue.Front()
		toEvictKey := toEvictNode.Value.(string) //nolint:errcheck // We only ever store string in this value.

		err := s.deleteFromDisk(toEvictKey, _completeBlob)
		if err != nil {
			return fmt.Errorf("delete from disk: %w", err)
		}
		s.evictQueue.Remove(toEvictNode)
		size := s.blobs[toEvictKey].size
		s.releaseSpace(size)
		delete(s.blobs, toEvictKey)
	}
	s.size += space

	latency := time.Since(startTime)
	s.metrics.Histogram("sync_eviction_latency", _syncEvictionLatencyBuckets).RecordDuration(latency)
	return nil
}

func (s *store) releaseSpace(space uint64) {
	if space > s.size {
		s.log.Error("Invariant violation - Store wants to release more disk space than actually reserved. Failing open by releasing all reserved space.")
		s.size = 0
		return
	}
	s.size -= space
}

// Fully deletes the disk state of a blob, including metadata. Works on any blob.
func (s *store) deleteFromDisk(key string, complete bool) error {
	dir := s.dirPath(key, complete)
	return os.RemoveAll(dir)
}

func (s *store) MarkComplete(key string) error {
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
	err = os.Rename(oldPathDir, newPathDir)
	if err != nil {
		return fmt.Errorf("move dir: %w", err)
	}
	b.complete = true
	if !b.evictionBanned {
		node := s.evictQueue.PushBack(key)
		b.node = node
	}

	s.tryDeleteImmovableMetadata(key)
	return nil
}

// Best-effort attempt to remove immovable metadata. Fail-open on failure
// to avoid an inconsistent state, as failure only costs a negligible amount
// of disk until the blob is evicted.
func (s *store) tryDeleteImmovableMetadata(key string) {
	mdList, err := s.listMetadataNoLock(key, storelib.BlobScopeAny)
	if err != nil {
		err = fmt.Errorf("list metadata: %w", err)
		s.log.With("error", err).Error("Failed to delete un-movable metadata upon marking a blob as complete")
		return
	}
	for _, md := range mdList {
		if md.Movable() {
			continue
		}
		mdFilePath := s.sidecarFilePath(key, _completeBlob, md.GetSuffix())
		err = os.Remove(mdFilePath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			err = fmt.Errorf("remove metadata file: %w", err)
			s.log.With("error", err).Error("Failed to delete un-movable metadata upon marking a blob as complete")
			continue
		}
	}
}

func (s *store) checkDiskIfUnevictable(key string, complete bool) (bool, error) {
	flagBlobPath := s.sidecarFilePath(key, complete, _evictionBannedFileName)
	unevictable, err := exists(flagBlobPath)
	if err != nil {
		return false, err
	}
	return unevictable, nil
}

func (s *store) Delete(key string, scope storelib.BlobScope) error {
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

	s.emitUsageMetrics()
	return nil
}

func (s *store) list(scope storelib.BlobScope) []string {
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

func (s *store) BanEviction(key string, scope storelib.BlobScope) error {
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

func (s *store) UnbanEviction(key string, scope storelib.BlobScope) error {
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

func (s *store) SetMetadata(key string, md metadata.Metadata, scope storelib.BlobScope) error {
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
	// Use a tmp file to ensure atomicity.
	tmpFilePath := mdFilePath + "-tmp"
	tmpFile, err := os.OpenFile(tmpFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, _defaultFilePerm)
	if err != nil {
		return fmt.Errorf("create tmp file for md: %w", err)
	}
	defer closers.Close(tmpFile)
	_, err = tmpFile.Write(mdData)
	if err != nil {
		return fmt.Errorf("write to tmp file: %w", err)
	}
	err = os.Rename(tmpFile.Name(), mdFilePath)
	if err != nil {
		return fmt.Errorf("rename tmp file: %w", err)
	}
	return nil
}

func (s *store) GetMetadata(key string, md metadata.Metadata, scope storelib.BlobScope) (ok bool, err error) {
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
	if err != nil {
		return false, fmt.Errorf("open metadata file: %w", err)
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

func (s *store) DeleteMetadata(key string, md metadata.Metadata, scope storelib.BlobScope) error {
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

func (s *store) ListMetadata(key string, scope storelib.BlobScope) ([]metadata.Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.listMetadataNoLock(key, scope)
}

func (s *store) listMetadataNoLock(key string, scope storelib.BlobScope) ([]metadata.Metadata, error) {
	b, ok := s.blobs[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	if err := isOutOfScope(b, scope); err != nil {
		return nil, err
	}

	res := make([]metadata.Metadata, 0)
	entries, err := os.ReadDir(s.dirPath(key, b.complete))
	if err != nil {
		return nil, fmt.Errorf("read metadata files from dir: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == _blobFileName || name == _evictionBannedFileName || name == _blobSizeFileName {
			continue
		}

		md := metadata.CreateFromSuffix(name)
		if md == nil {
			s.log.With("key", key, "file_name", name).Warn("Found file in blob dir that does not successfully parse as a metadata file")
			continue
		}
		res = append(res, md)
	}
	return res, nil
}

func (s *store) WriteAtMetadata(key string, md metadata.Metadata, p []byte, off int64, scope storelib.BlobScope) error {
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
	mdFile, err := os.OpenFile(mdFilePath, os.O_WRONLY, _defaultFilePerm)
	if errors.Is(err, os.ErrNotExist) {
		return errors.New("metadata does not exist")
	}
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer closers.Close(mdFile)

	_, err = mdFile.WriteAt(p, off)
	if err != nil {
		return fmt.Errorf("write at: %w", err)
	}

	return nil
}

// used during testing
func (s *store) evictionOrder() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	evictionOrder := make([]string, 0)
	for curr := s.evictQueue.Front(); curr != nil; curr = curr.Next() {
		currKey := curr.Value.(string) //nolint:errcheck // We only ever store string in this value.
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

func isOutOfScope(b *blob, scope storelib.BlobScope) error {
	if (b.complete && scope == storelib.BlobScopeIncomplete) || (!b.complete && scope == storelib.BlobScopeComplete) {
		return storelib.ErrOutOfScope
	}
	return nil
}

func (s *store) emitUsageMetrics() {
	s.metrics.Gauge("num_entries").Update(float64(len(s.blobs)))
	s.metrics.Gauge("size_bytes").Update(float64(s.size))
}
