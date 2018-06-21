package store

import (
	"io/ioutil"
	"os"

	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/uber-go/tally"
)

// CAStoreConfigFixture returns config for CAStore for testing purposes.
func CAStoreConfigFixture() (CAStoreConfig, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	upload, err := ioutil.TempDir("/tmp", "upload")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(upload) })

	cache, err := ioutil.TempDir("/tmp", "cache")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(cache) })

	return CAStoreConfig{
		UploadDir: upload,
		CacheDir:  cache,
	}, cleanup.Run
}

// CAStoreFixture returns a CAStore for testing purposes.
func CAStoreFixture() (*CAStore, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	config, c := CAStoreConfigFixture()
	cleanup.Add(c)

	s, err := NewCAStore(config, tally.NoopScope)
	if err != nil {
		panic(err)
	}
	return s, cleanup.Run
}

// CADownloadStoreFixture returns a CADownloadStore for testing purposes.
func CADownloadStoreFixture() (*CADownloadStore, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	download, err := ioutil.TempDir("/tmp", "download")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(download) })

	cache, err := ioutil.TempDir("/tmp", "cache")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(cache) })

	config := CADownloadStoreConfig{
		DownloadDir: download,
		CacheDir:    cache,
	}
	s, err := NewCADownloadStore(config, tally.NoopScope)
	if err != nil {
		panic(err)
	}
	return s, cleanup.Run
}
