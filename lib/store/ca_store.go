package store

import (
	"fmt"
	"hash"
	"io"
	"os"
	"path"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/lib/store/base"
	"github.com/andres-erbsen/clock"
	"github.com/docker/distribution/uuid"
	"github.com/spaolacci/murmur3"
	"github.com/uber-go/tally"
)

// CAStore allows uploading / caching content-addressable files.
type CAStore struct {
	*uploadStore
	*cacheStore
	cleanup *cleanupManager
}

// NewCAStore creates a new CAStore.
func NewCAStore(config CAStoreConfig, stats tally.Scope) (*CAStore, error) {
	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "castore",
	})

	uploadStore, err := newUploadStore(config.UploadDir)
	if err != nil {
		return nil, fmt.Errorf("new upload store: %s", err)
	}

	cacheBackend := base.NewCASFileStoreWithLRUMap(config.Capacity, clock.New())
	cacheStore, err := newCacheStore(config.CacheDir, cacheBackend)
	if err != nil {
		return nil, fmt.Errorf("new cache store: %s", err)
	}

	if err := initCASVolumes(config.CacheDir, config.Volumes); err != nil {
		return nil, fmt.Errorf("init cas volumes: %s", err)
	}

	cleanup, err := newCleanupManager(clock.New(), stats)
	if err != nil {
		return nil, fmt.Errorf("new cleanup manager: %s", err)
	}
	cleanup.addJob("upload", config.UploadCleanup, uploadStore.newFileOp())
	cleanup.addJob("cache", config.CacheCleanup, cacheStore.newFileOp())

	return &CAStore{uploadStore, cacheStore, cleanup}, nil
}

// Close terminates any goroutines started by s.
func (s *CAStore) Close() {
	s.cleanup.stop()
}

// MoveUploadFileToCache commits uploadName as cacheName. Clients are expected
// to validate the content of the upload file matches the cacheName digest.
func (s *CAStore) MoveUploadFileToCache(uploadName, cacheName string) error {
	uploadPath, err := s.uploadStore.newFileOp().GetFilePath(uploadName)
	if err != nil {
		return err
	}
	defer s.DeleteUploadFile(uploadName)
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
	tmp := fmt.Sprintf("%s.%s", name, uuid.Generate().String())
	if err := s.CreateUploadFile(tmp, 0); err != nil {
		return fmt.Errorf("create upload file: %s", err)
	}
	defer s.DeleteUploadFile(tmp)

	w, err := s.GetUploadFileReadWriter(tmp)
	if err != nil {
		return fmt.Errorf("get upload writer: %s", err)
	}
	defer w.Close()

	if err := write(w); err != nil {
		return err
	}

	if _, err := w.Seek(0, 0); err != nil {
		return fmt.Errorf("seek: %s", err)
	}
	actual, err := core.NewDigester().FromReader(w)
	if err != nil {
		return fmt.Errorf("compute digest: %s", err)
	}
	expected, err := core.NewSHA256DigestFromHex(name)
	if err != nil {
		return fmt.Errorf("new digest from file name: %s", err)
	}
	if actual != expected {
		return fmt.Errorf("failed to verify data: digests do not match")
	}

	if err := s.MoveUploadFileToCache(tmp, name); err != nil && !os.IsExist(err) {
		return fmt.Errorf("move upload file to cache: %s", err)
	}
	return nil
}

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
