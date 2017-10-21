package dockerregistry

import (
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"

	"code.uber.internal/infra/kraken/lib/store"
	"github.com/uber-go/tally"
)

// TODO (@evelynl): this should use a generated mock or an actual transferer fixture
var errMockError = errors.New("MockTorrent")

type mockImageTransferer struct{}

func (mc *mockImageTransferer) Download(digest string) (io.ReadCloser, error) {
	return nil, errMockError
}
func (mc *mockImageTransferer) Upload(digest string, reader io.Reader, size int64) error { return nil }
func (mc *mockImageTransferer) GetManifest(repo, tag string) (readCloser io.ReadCloser, err error) {
	return nil, errMockError
}
func (mc *mockImageTransferer) PostManifest(repo, tag, digest string, reader io.Reader) error {
	return nil
}
func (mc *mockImageTransferer) Close() error { return nil }

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

	localStore, cleanupStore := store.LocalFileStoreFixture()
	cleanup := func() {
		cleanupStore()
		os.RemoveAll(tag)
	}

	sd, err := NewKrakenStorageDriver(config, localStore, &mockImageTransferer{}, tally.NoopScope)
	if err != nil {
		cleanup()
		log.Panic(err)
	}
	return sd, cleanup
}
