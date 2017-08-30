package store

import (
	"io/ioutil"
	"log"
	"os"
	"time"
)

func localStoreFixture(
	refcountable bool, trashDeletionConfig TrashDeletionConfig) (s *LocalStore, cleanup func()) {

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
	s, err = NewLocalStore(config, refcountable)
	if err != nil {
		log.Fatal(err)
	}

	return s, cleanup
}

// LocalStoreFixture returns a LocalStore using temp directories.
func LocalStoreFixture() (s *LocalStore, cleanup func()) {
	return localStoreFixture(false, TrashDeletionConfig{})
}

// LocalStoreWithRefcountFixture returns a refcountable LocalStore using temp
// directories.
func LocalStoreWithRefcountFixture() (s *LocalStore, cleanup func()) {
	return localStoreFixture(true, TrashDeletionConfig{})
}

// LocalStoreWithTrashDeletionFixture returns a LocalStore with trash deletion
// occuring at the given interval.
func LocalStoreWithTrashDeletionFixture(interval time.Duration) (s *LocalStore, cleanup func()) {
	return localStoreFixture(false, TrashDeletionConfig{
		Enable:   true,
		Interval: interval,
	})
}
