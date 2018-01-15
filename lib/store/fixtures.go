package store

import (
	"io/ioutil"
	"os"
	"time"

	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/andres-erbsen/clock"
)

func localFileStoreFixture(
	refcountable bool, trashDeletionConfig TrashDeletionConfig) (*LocalFileStore, func()) {

	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	upload, err := ioutil.TempDir("/tmp", "upload")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() {
		os.RemoveAll(upload)
	})
	download, err := ioutil.TempDir("/tmp", "download")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() {
		os.RemoveAll(download)
	})
	cache, err := ioutil.TempDir("/tmp", "cache")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() {
		os.RemoveAll(cache)
	})
	trash, err := ioutil.TempDir("/tmp", "trash")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() {
		os.RemoveAll(trash)
	})

	config := &Config{
		UploadDir:     upload,
		DownloadDir:   download,
		CacheDir:      cache,
		TrashDir:      trash,
		TrashDeletion: trashDeletionConfig,
	}
	s, err := NewLocalFileStore(config, refcountable)
	if err != nil {
		panic(err)
	}

	return s, cleanup.Run
}

// LocalFileStoreFixture returns a LocalFileStore using temp directories.
func LocalFileStoreFixture() (s *LocalFileStore, cleanup func()) {
	return localFileStoreFixture(false, TrashDeletionConfig{})
}

// LocalFileStoreWithRefcountFixture returns a refcountable LocalFileStore using temp
// directories.
func LocalFileStoreWithRefcountFixture() (s *LocalFileStore, cleanup func()) {
	return localFileStoreFixture(true, TrashDeletionConfig{})
}

// LocalFileStoreWithTrashDeletionFixture returns a LocalFileStore with trash deletion
// occuring at the given interval.
func LocalFileStoreWithTrashDeletionFixture(interval time.Duration) (s *LocalFileStore, cleanup func()) {
	return localFileStoreFixture(false, TrashDeletionConfig{
		Enable:   true,
		Interval: interval,
	})
}

// OriginFileStoreFixture returns a origin file store.
func OriginFileStoreFixture(clk clock.Clock) (*OriginLocalFileStore, func()) {
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	upload, err := ioutil.TempDir("/tmp", "upload")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() {
		os.RemoveAll(upload)
	})

	cache, err := ioutil.TempDir("/tmp", "cache")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() {
		os.RemoveAll(cache)
	})

	config := OriginConfig{
		UploadDir: upload,
		CacheDir:  cache,
		TTI:       time.Hour * 24,
	}
	s, err := NewOriginFileStore(config, clk)
	if err != nil {
		panic(err)
	}

	return s, cleanup.Run
}
