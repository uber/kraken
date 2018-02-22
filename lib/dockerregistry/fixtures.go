package dockerregistry

import (
	"errors"
	"io/ioutil"
	"os"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/uber-go/tally"
)

// TODO (@evelynl): this should use a generated mock or an actual transferer fixture
var errMockError = errors.New("MockTorrent")

type mockImageTransferer struct{}

func (mc *mockImageTransferer) Download(name string) (store.FileReader, error) {
	return nil, errMockError
}
func (mc *mockImageTransferer) Upload(name string, blob store.FileReader, size int64) error {
	return nil
}
func (mc *mockImageTransferer) GetTag(repo, tag string) (core.Digest, error) {
	return core.Digest{}, errMockError
}
func (mc *mockImageTransferer) PostTag(repo, tag string, manifestDigest core.Digest) error {
	return nil
}
func (mc *mockImageTransferer) Close() error { return nil }

// StorageDriverFixture creates a storage driver and return a cleanup function
func StorageDriverFixture() (*KrakenStorageDriver, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	tag, err := ioutil.TempDir("/tmp", "tag")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(tag) })

	config := Config{
		TagDir: tag,
	}

	localStore, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	sd, err := NewKrakenStorageDriver(config, localStore, &mockImageTransferer{}, tally.NoopScope)
	if err != nil {
		panic(err)
	}
	cleanup.Add(sd.Close)

	return sd, cleanup.Run
}
