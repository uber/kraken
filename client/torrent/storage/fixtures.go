package storage

import "code.uber.internal/infra/kraken/client/store"

// TorrentArchiveFixture creates a new TorrentArchive and returns the archive with a cleanup function
func TorrentArchiveFixture() (TorrentArchive, func()) {
	localStore, cleanup := store.LocalStoreFixture()
	return NewLocalTorrentArchive(localStore), cleanup
}
