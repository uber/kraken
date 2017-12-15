package storage

import (
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/testutil"
)

// TorrentArchiveFixture creates a new TorrentArchive and returns the archive with a cleanup function
func TorrentArchiveFixture() (TorrentArchive, func()) {
	localStore, cleanup := store.LocalFileStoreFixture()
	return NewAgentTorrentArchive(localStore, nil), cleanup
}

// TorrentFixture returns a Torrent for the given size and piece length.
func TorrentFixture(size, pieceLength uint64) (Torrent, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	mic := metainfoclient.TestClient()

	ta := NewAgentTorrentArchive(fs, mic)

	mi := torlib.CustomMetaInfoFixture(size, pieceLength)
	if err := mic.Upload(mi); err != nil {
		panic(err)
	}
	tor, err := ta.GetTorrent(mi.Name())
	if err != nil {
		panic(err)
	}

	return tor, cleanup.Run
}
