package dockerregistry

import (
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"

	"github.com/uber-go/tally"
)

// StorageDriverFixture creates a storage driver for testing purposes.
func StorageDriverFixture() (*KrakenStorageDriver, func()) {
	cas, cleanup := store.CAStoreFixture()
	sd := NewReadWriteStorageDriver(
		Config{}, cas, transfer.NewTestTransferer(cas), tally.NoopScope)
	return sd, cleanup
}
