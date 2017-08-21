package storage

import (
	"io/ioutil"
	"log"
	"os"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/configuration"
)

// TorrentArchiveFixture creates a new TorrentArchive and returns the archive with a cleanup function
func TorrentArchiveFixture() (TorrentArchive, func()) {
	var upload, download, cache, trash string
	cleanup := func() {
		os.RemoveAll(upload)
		os.RemoveAll(download)
		os.RemoveAll(cache)
		os.RemoveAll(trash)
	}

	upload, err := ioutil.TempDir("/tmp", "upload")
	if err != nil {
		cleanup()
		log.Panic(err)
	}
	download, err = ioutil.TempDir("/tmp", "download")
	if err != nil {
		cleanup()
		log.Panic(err)
	}
	cache, err = ioutil.TempDir("/tmp", "cache")
	if err != nil {
		cleanup()
		log.Panic(err)
	}
	trash, err = ioutil.TempDir("/tmp", "trash")
	if err != nil {
		cleanup()
		log.Panic(err)
	}

	config := &configuration.Config{
		UploadDir:   upload,
		DownloadDir: download,
		CacheDir:    cache,
		TrashDir:    trash,
	}

	localStore := store.NewLocalStore(config)

	return NewLocalTorrentArchive(localStore), cleanup
}
