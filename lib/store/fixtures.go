package store

import (
	"io/ioutil"
	"os"

	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
)

// ConfigFixture returns a Config with initialized temporary directories.
func ConfigFixture() (Config, func()) {
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	upload, err := ioutil.TempDir("/tmp", "upload")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(upload) })

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

	config := Config{
		UploadDir:   upload,
		DownloadDir: download,
		CacheDir:    cache,
	}.applyDefaults()

	return config, cleanup.Run
}

// LocalFileStoreFixture returns a LocalFileStore using temp directories.
func LocalFileStoreFixture() (*LocalFileStore, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	config, c := ConfigFixture()
	cleanup.Add(c)

	fs, err := NewLocalFileStore(config, tally.NewTestScope("", nil), false)
	if err != nil {
		panic(err)
	}
	cleanup.Add(fs.Close)

	return fs, cleanup.Run
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
	}
	s, err := NewOriginFileStore(config, clk)
	if err != nil {
		panic(err)
	}

	return s, cleanup.Run
}
