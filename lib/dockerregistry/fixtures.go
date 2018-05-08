package dockerregistry

import (
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/uber-go/tally"
)

// StorageDriverFixture creates a storage driver and return a cleanup function
func StorageDriverFixture() (*KrakenStorageDriver, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	sd, err := NewKrakenStorageDriver(Config{}, fs, transfer.NewTestTransferer(fs), tally.NoopScope)
	if err != nil {
		panic(err)
	}

	return sd, cleanup.Run
}
