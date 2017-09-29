package store

import (
	"io/ioutil"
	"log"
	"os"
	"time"
)

func localFileStoreFixture(
	refcountable bool, trashDeletionConfig TrashDeletionConfig) (s *LocalFileStore, cleanup func()) {

	var upload, download, cache, trash string
	cleanup = func() {
		os.RemoveAll(upload)
		os.RemoveAll(download)
		os.RemoveAll(cache)
		os.RemoveAll(trash)
	}
	defer func() {
		if err := recover(); err != nil {
			cleanup()
		}
	}()

	upload, err := ioutil.TempDir("/tmp", "upload")
	if err != nil {
		log.Panic(err)
	}
	download, err = ioutil.TempDir("/tmp", "download")
	if err != nil {
		log.Panic(err)
	}
	cache, err = ioutil.TempDir("/tmp", "cache")
	if err != nil {
		log.Panic(err)
	}
	trash, err = ioutil.TempDir("/tmp", "trash")
	if err != nil {
		log.Panic(err)
	}

	config := &Config{
		UploadDir:     upload,
		DownloadDir:   download,
		CacheDir:      cache,
		TrashDir:      trash,
		TrashDeletion: trashDeletionConfig,
	}
	s, err = NewLocalFileStore(config, refcountable)
	if err != nil {
		log.Fatal(err)
	}

	return s, cleanup
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
