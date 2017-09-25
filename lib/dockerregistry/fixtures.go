package dockerregistry

import (
	"errors"
	"io/ioutil"
	"log"
	"os"

	"code.uber.internal/infra/kraken/client/store"
	"github.com/uber-go/tally"
)

// TODO (@evelynl): depending on D1135521, you should remove this
var errMockError = errors.New("MockTorrent")

type mockTorrentClient struct{}

func (mc *mockTorrentClient) DownloadTorrent(name string) error                   { return errMockError }
func (mc *mockTorrentClient) CreateTorrentFromFile(name, filepath string) error   { return nil }
func (mc *mockTorrentClient) GetManifest(repo, tag string) (string, error)        { return "", errMockError }
func (mc *mockTorrentClient) PostManifest(repo, tag, manifestDigest string) error { return nil }
func (mc *mockTorrentClient) Close() error                                        { return nil }

// StorageDriverFixture creates a storage driver and return a cleanup function
func StorageDriverFixture() (*KrakenStorageDriver, func()) {
	var tag string

	tag, err := ioutil.TempDir("/tmp", "tag")
	if err != nil {
		os.RemoveAll(tag)
		log.Panic(err)
	}

	config := &Config{
		TagDir: tag,
	}

	localStore, cleanupStore := store.LocalStoreFixture()
	cleanup := func() {
		cleanupStore()
		os.RemoveAll(tag)
	}

	sd, err := NewKrakenStorageDriver(config, localStore, &mockTorrentClient{}, tally.NoopScope)
	if err != nil {
		cleanup()
		log.Panic(err)
	}
	return sd, cleanup
}
