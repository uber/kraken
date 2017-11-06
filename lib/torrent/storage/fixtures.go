package storage

import "code.uber.internal/infra/kraken/lib/store"

// TorrentArchiveFixture creates a new TorrentArchive and returns the archive with a cleanup function
func TorrentArchiveFixture() (TorrentArchive, func()) {
	localStore, cleanup := store.LocalFileStoreFixture()
	return NewLocalTorrentArchive(localStore, nil), cleanup
}
