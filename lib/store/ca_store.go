// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package store

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/docker/distribution/uuid"
	"github.com/spaolacci/murmur3"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hrw"
	"github.com/uber/kraken/lib/store/base"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/cache"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/log"
)

const _drainDuration = 100 * time.Millisecond

var drainDurationBuckets = append(
	tally.DurationBuckets{0},
	tally.MustMakeExponentialDurationBuckets(100*time.Millisecond, 2, 12)...,
)

// drainItem represents an item in the drain queue for async disk writing.
type drainItem struct {
	entry   *cache.MemoryEntry
	retries int
	errs    []error
}

type drain struct {
	queue     *list.List
	mu        sync.Mutex
	stopChan  chan struct{}
	wg        sync.WaitGroup
	histogram tally.Histogram
}

// CAStore allows uploading / caching content-addressable files.
type CAStore struct {
	config CAStoreConfig
	stats  tally.Scope
	clk    clock.Clock

	*uploadStore
	*cacheStore
	cleanup *cleanupManager

	memCache *cache.BlobMemoryCache

	drain       *drain
	ttlStopChan chan struct{}
	ttlWg       sync.WaitGroup
}

// NewCAStore creates a new CAStore.
func NewCAStore(config CAStoreConfig, stats tally.Scope) (*CAStore, error) {
	return newCAStore(config, stats, clock.New())
}

// newCAStore creates a new CAStore with clock injected
func newCAStore(config CAStoreConfig, stats tally.Scope, clk clock.Clock) (*CAStore, error) {
	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "castore",
	})

	uploadStore, err := newUploadStore(config.UploadDir, config.ReadPartSize, config.WritePartSize)
	if err != nil {
		return nil, fmt.Errorf("new upload store: %s", err)
	}

	cacheBackend := base.NewCASFileStoreWithLRUMap(config.Capacity, clk)
	cacheStore, err := newCacheStore(config.CacheDir, cacheBackend, config.ReadPartSize)
	if err != nil {
		return nil, fmt.Errorf("new cache store: %s", err)
	}

	if err := initCASVolumes(config.CacheDir, config.Volumes); err != nil {
		return nil, fmt.Errorf("init cas volumes: %s", err)
	}

	cleanup, err := newCleanupManager(clk, stats)
	if err != nil {
		return nil, fmt.Errorf("new cleanup manager: %s", err)
	}
	cleanup.addJob("upload", config.UploadCleanup, uploadStore.newFileOp())
	cleanup.addJob("cache", config.CacheCleanup, cacheStore.newFileOp())

	cas := &CAStore{
		config:      config,
		stats:       stats,
		clk:         clk,
		uploadStore: uploadStore,
		cacheStore:  cacheStore,
		cleanup:     cleanup,
	}

	if config.MemoryCache.Enabled {
		memCache := createMemoryCache(&config, stats)
		cas.memCache = memCache
		initMemCacheCleanupJob(cas)
		startDrainWorkers(cas)
	}

	return cas, nil
}

func createMemoryCache(config *CAStoreConfig, stats tally.Scope) *cache.BlobMemoryCache {
	return cache.NewBlobMemoryCache(cache.BlobMemoryCacheConfig{
		MaxSize: config.MemoryCache.MaxSize,
	}, stats)
}

func initMemCacheCleanupJob(cas *CAStore) {
	cas.ttlStopChan = make(chan struct{})
	cas.ttlWg.Add(1)
	go cas.memoryCacheCleanupWorker()
}

func startDrainWorkers(cas *CAStore) {
	cas.drain = &drain{
		histogram: cas.stats.Histogram("drain_duration", drainDurationBuckets),
		stopChan:  make(chan struct{}),
		queue:     list.New(),
	}
	for range cas.config.MemoryCache.DrainWorkers {
		cas.drain.wg.Add(1)
		go cas.drainWorker()
	}
}

// Close terminates any goroutines started by s.
func (s *CAStore) Close() {
	if s.drain != nil && s.drain.stopChan != nil {
		close(s.drain.stopChan)
		s.drain.wg.Wait()
	}

	if s.ttlStopChan != nil {
		close(s.ttlStopChan)
		s.ttlWg.Wait()
	}

	s.cleanup.stop()
}

// MoveUploadFileToCache commits uploadName as cacheName. Clients are expected
// to validate the content of the upload file matches the cacheName digest.
func (s *CAStore) MoveUploadFileToCache(uploadName, cacheName string) error {
	uploadPath, err := s.uploadStore.newFileOp().GetFilePath(uploadName)
	if err != nil {
		return err
	}
	defer s.deferDeleteUploadFile(uploadName)()

	f, err := s.uploadStore.newFileOp().GetFileReader(uploadName, s.uploadStore.readPartSize)
	if err != nil {
		return fmt.Errorf("get file reader %s: %s", uploadName, err)
	}
	defer closers.Close(f)
	if err := s.verify(f, cacheName); err != nil {
		return fmt.Errorf("verify digest: %s", err)
	}

	return s.cacheStore.newFileOp().MoveFileFrom(cacheName, s.cacheStore.state, uploadPath)
}

// CreateCacheFile initializes a cache file for name from r. name should be a raw
// hex sha256 digest, and the contents of r must hash to name.
func (s *CAStore) CreateCacheFile(name string, r io.Reader) error {
	return s.WriteCacheFile(name, func(w FileReadWriter) error {
		_, err := io.Copy(w, r)
		return err
	})
}

// WriteCacheFile initializes a cache file for name by passing a temporary
// upload file writer to the write function.
func (s *CAStore) WriteCacheFile(name string, write func(w FileReadWriter) error) error {
	return s.writeCacheFile(name, write, false, 0)
}

// this function writes cache file with an option to write metadata alongside
func (s *CAStore) writeCacheFile(name string, write func(w FileReadWriter) error, addMetadata bool, pieceLength int64) error {
	tmp := fmt.Sprintf("%s.%s", name, uuid.Generate().String())
	if err := s.CreateUploadFile(tmp, 0); err != nil {
		return fmt.Errorf("create upload file: %s", err)
	}
	defer s.deferDeleteUploadFile(tmp)()

	w, err := s.GetUploadFileReadWriter(tmp)
	if err != nil {
		return fmt.Errorf("get upload writer: %s", err)
	}
	defer closers.Close(w)

	if err := write(w); err != nil {
		return err
	}
	if err := s.MoveUploadFileToCache(tmp, name); err != nil && !os.IsExist(err) {
		return fmt.Errorf("move upload file to cache: %s", err)
	}
	if addMetadata {
		return s.generateMetadataFromFile(name, pieceLength)
	}
	return nil
}

// WriteBlobToCacheWithMetaInfo writes a blob and its metadata to disk,
// potentially going through a write-through memory cache, if memory is available.
func (s *CAStore) WriteBlobToCacheWithMetaInfo(
	name string,
	size uint64,
	write func(w FileReadWriter) error,
	pieceLength int64) error {
	if s.config.MemoryCache.Enabled && s.memCache.TryReserve(size) {
		log.With("name", name, "size", size).Debug("successfully reserved cache")
		err := s.addToMemoryCache(name, write, size, pieceLength)
		if err == nil {
			return nil
		}
		log.With("blob", name).Errorf("error while trying to add the blob to memory cache: %w", err)
		s.memCache.ReleaseReservation(size)
	}
	addMetadata := true
	return s.writeCacheFile(name, write, addMetadata, pieceLength)
}

// CheckInMemCache returns true if the blob is present in memcache
// Used in tests
func (s *CAStore) CheckInMemCache(name string) bool {
	return s.memCache.Get(name) != nil
}

func (s *CAStore) addToMemoryCache(
	name string,
	write func(w FileReadWriter) error,
	size uint64,
	pieceLength int64,
) error {
	tmpWriter := base.NewBufferReadWriter(size)

	if err := write(tmpWriter); err != nil {
		return err
	}

	data := tmpWriter.Bytes()
	metaInfo, err := s.generateMetadataFromBytes(name, data, pieceLength)
	if err != nil {
		return fmt.Errorf("generating metainfo: %w", err)
	}

	entry := &cache.MemoryEntry{
		Name:      name,
		Data:      data,
		MetaInfo:  metaInfo,
		CreatedAt: s.clk.Now(),
	}

	if added := s.memCache.Add(entry); !added {
		// Multiple goroutines are trying to add the same blob (which should never happen).
		return fmt.Errorf("entry already in in-memory cache")
	}

	log.With("name", name, "size", entry.Size(), "cap", cap(data)).Debug("successfully added to cache")

	s.addItemForDiskSync(&drainItem{
		entry:   entry,
		retries: 0,
	})
	return nil
}

func (s *CAStore) generateMetadataFromBytes(name string, data []byte, pieceLength int64) (*core.MetaInfo, error) {
	digest, err := core.NewSHA256DigestFromHex(name)
	if err != nil {
		return nil, fmt.Errorf("new digest from hex: %s", err)
	}
	metaInfo, err := core.NewMetaInfo(digest, bytes.NewReader(data), pieceLength)
	if err != nil {
		return nil, fmt.Errorf("generate metainfo: %w", err)
	}
	return metaInfo, nil
}

func (s *CAStore) generateMetadataFromFile(name string, pieceLength int64) error {
	d, err := core.NewSHA256DigestFromHex(name)
	if err != nil {
		return fmt.Errorf("get digest from file: %w", err)
	}
	f, err := s.GetCacheFileReader(name)
	if err != nil {
		return fmt.Errorf("get cache file: %w", err)
	}
	mi, err := core.NewMetaInfo(d, f, pieceLength)
	if err != nil {
		return fmt.Errorf("create metainfo: %w", err)
	}
	if _, err := s.SetCacheFileMetadata(d.Hex(), metadata.NewTorrentMeta(mi)); err != nil {
		return fmt.Errorf("set metainfo: %w", err)
	}
	return nil
}

func (s *CAStore) addItemForDiskSync(item *drainItem) {
	s.drain.mu.Lock()
	defer s.drain.mu.Unlock()
	s.drain.queue.PushBack(item)
}

// verify verifies that name is a valid SHA256 digest, and checks if the given
// blob content matches the digset unless explicitly skipped.
func (s *CAStore) verify(r io.Reader, name string) error {
	// Verify that expected name is a valid SHA256 digest.
	expected, err := core.NewSHA256DigestFromHex(name)
	if err != nil {
		return fmt.Errorf("new digest from file name: %s", err)
	}

	if !s.config.SkipHashVerification {
		digester := core.NewDigester()
		computed, err := digester.FromReader(r)
		if err != nil {
			return fmt.Errorf("calculate digest: %s", err)
		}
		if computed != expected {
			return fmt.Errorf("computed digest %s doesn't match expected value %s", computed, expected)
		}
	}
	return nil
}

func (s *CAStore) memoryCacheCleanupWorker() {
	defer s.ttlWg.Done()

	ticker := s.clk.Ticker(s.config.MemoryCache.TTLInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.cleanupMemoryCacheExpiredEntries()
		case <-s.ttlStopChan:
			return
		}
	}
}

func (s *CAStore) cleanupMemoryCacheExpiredEntries() {
	expiredNames := s.memCache.GetExpiredEntries(s.clk.Now(), s.config.MemoryCache.TTL)

	if len(expiredNames) > 0 {
		s.memCache.RemoveBatch(expiredNames)
	}
}

func (s *CAStore) drainWorker() {
	defer s.drain.wg.Done()

	ticker := s.clk.Ticker(_drainDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.drainNext()
		case <-s.drain.stopChan:
			return
		}
	}
}

func (s *CAStore) dequeueNextDrainItem() *drainItem {
	s.drain.mu.Lock()
	defer s.drain.mu.Unlock()

	if s.drain.queue.Len() == 0 {
		return nil
	}

	elem := s.drain.queue.Front()
	s.drain.queue.Remove(elem)
	item, ok := elem.Value.(*drainItem)
	if !ok {
		// Shouldn't happen, but good to check
		return nil
	}
	return item
}

func (s *CAStore) drainNext() {
	item := s.dequeueNextDrainItem()
	if item == nil {
		return
	}

	err := s.writeDrainItemToDisk(item.entry)
	if err != nil {
		if item.retries < s.config.MemoryCache.DrainMaxRetries {
			s.addItemForDiskSync(&drainItem{
				entry:   item.entry,
				retries: item.retries + 1,
				errs:    append(item.errs, err),
			})
			return
		}
		s.memCache.Remove(item.entry.Name)
		s.stats.Counter("drain_error").Inc(1)
		log.With("name", item.entry.Name, "drain_errors", errors.Join(append(item.errs, err)...)).
			Errorf("Failed to drain blob from mem cache to disk after %v retries", s.config.MemoryCache.DrainMaxRetries)
		return
	}

	s.drain.histogram.RecordDuration(s.clk.Now().Sub(item.entry.CreatedAt))
	s.memCache.Remove(item.entry.Name)
}

func (s *CAStore) writeDrainItemToDisk(entry *cache.MemoryEntry) error {
	if err := s.WriteCacheFile(entry.Name, func(w FileReadWriter) error {
		_, err := w.Write(entry.Data)
		return err
	}); err != nil {
		return fmt.Errorf("write blob: %s", err)
	}

	tm := metadata.NewTorrentMeta(entry.MetaInfo)
	if _, err := s.SetCacheFileMetadata(entry.Name, tm); err != nil {
		return fmt.Errorf("write metadata: %s", err)
	}

	return nil
}

// GetCacheFileReader overrides cacheStore.GetCacheFileReader to check
// memory cache first before reading from disk.
func (s *CAStore) GetCacheFileReader(name string) (FileReader, error) {
	if s.memCache != nil {
		if entry := s.memCache.Get(name); entry != nil {
			return NewBufferFileReader(entry.Data), nil
		}
	}

	return s.cacheStore.GetCacheFileReader(name)
}

// GetCacheFileMetadata overrides cacheStore.GetCacheFileMetadata to serve
// TorrentMeta from memory cache when available.
func (s *CAStore) GetCacheFileMetadata(name string, md metadata.Metadata) error {
	if s.memCache != nil && md.GetSuffix() == metadata.GetTorrentMetadataSuffix() {
		if entry := s.memCache.Get(name); entry != nil {
			if entry.MetaInfo == nil {
				// shouldn't happen, but good to check
				return fmt.Errorf("entry %s doesn't have any metainfo", entry.Name)
			}
			// Serialize and deserialize for consistency with disk behavior
			b, err := entry.MetaInfo.Serialize()
			if err != nil {
				return fmt.Errorf("serialize metainfo: %s", err)
			}
			return md.Deserialize(b)
		}
	}

	// Fallback to disk
	return s.cacheStore.GetCacheFileMetadata(name, md)
}

// GetCacheFileStat overrides cacheStore.GetCacheFileStat to check memory cache first.
func (s *CAStore) GetCacheFileStat(name string) (os.FileInfo, error) {
	if s.memCache != nil {
		if entry := s.memCache.Get(name); entry != nil {
			return &memoryFileInfo{
				name:    name,
				size:    int64(entry.Size()),
				modTime: entry.CreatedAt,
			}, nil
		}
	}
	return s.cacheStore.GetCacheFileStat(name)
}

// ListCacheFiles overrides cacheStore.ListCacheFiles to include memory cache entries.
func (s *CAStore) ListCacheFiles() ([]string, error) {
	diskFiles, err := s.cacheStore.ListCacheFiles()
	if err != nil {
		return nil, err
	}

	if s.memCache == nil {
		return diskFiles, nil
	}
	memNames := s.memCache.ListNames()
	allFiles := make(map[string]bool)
	for _, name := range diskFiles {
		allFiles[name] = true
	}
	for _, name := range memNames {
		allFiles[name] = true
	}

	result := make([]string, 0, len(allFiles))
	for name := range allFiles {
		result = append(result, name)
	}

	return result, nil
}

var _ os.FileInfo = &memoryFileInfo{}

// memoryFileInfo implements os.FileInfo for memory cache entries.
type memoryFileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (f *memoryFileInfo) Name() string       { return f.name }
func (f *memoryFileInfo) Size() int64        { return f.size }
func (f *memoryFileInfo) Mode() os.FileMode  { return os.ModePerm }
func (f *memoryFileInfo) ModTime() time.Time { return f.modTime }
func (f *memoryFileInfo) IsDir() bool        { return false }
func (f *memoryFileInfo) Sys() interface{}   { return nil }

func initCASVolumes(dir string, volumes []Volume) error {
	if len(volumes) == 0 {
		return nil
	}

	rendezvousHash := hrw.NewRendezvousHash(
		func() hash.Hash { return murmur3.New64() },
		hrw.UInt64ToFloat64)

	for _, v := range volumes {
		if _, err := os.Stat(v.Location); err != nil {
			return fmt.Errorf("verify volume: %s", err)
		}
		rendezvousHash.AddNode(v.Location, v.Weight)
	}

	// Create 256 symlinks under dir.
	for subdirIndex := 0; subdirIndex < 256; subdirIndex++ {
		subdirName := fmt.Sprintf("%02X", subdirIndex)
		nodes := rendezvousHash.GetOrderedNodes(subdirName, 1)
		if len(nodes) != 1 {
			return fmt.Errorf("calculate volume for subdir: %s", subdirName)
		}
		sourcePath := path.Join(nodes[0].Label, path.Base(dir), subdirName)
		if err := os.MkdirAll(sourcePath, 0775); err != nil {
			return fmt.Errorf("volume source path: %s", err)
		}
		targetPath := path.Join(dir, subdirName)
		if err := createOrUpdateSymlink(sourcePath, targetPath); err != nil {
			return fmt.Errorf("symlink to volume: %s", err)
		}
	}

	return nil
}
