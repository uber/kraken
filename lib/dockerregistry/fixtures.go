package dockerregistry

import (
	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/lib/store"

	"github.com/uber-go/tally"
)

// StorageDriverFixture creates a storage driver for testing purposes.
func StorageDriverFixture() (*KrakenStorageDriver, func()) {
	cas, cleanup := store.CAStoreFixture()
	sd := NewReadWriteStorageDriver(
		Config{}, cas, transfer.NewTestTransferer(cas), tally.NoopScope)
	return sd, cleanup
}
