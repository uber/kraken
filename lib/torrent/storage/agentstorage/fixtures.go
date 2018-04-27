package agentstorage

import (
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/uber-go/tally"
)

// TorrentArchiveFixture returns a TorrrentArchive for testing purposes.
func TorrentArchiveFixture() (*TorrentArchive, func()) {
	fs, cleanup := store.LocalFileStoreFixture()
	archive := DefaultTorrentArchive(tally.NoopScope, fs, nil)
	return archive, cleanup
}

// TorrentFixture returns a Torrent for the given metainfo for testing purposes.
func TorrentFixture(mi *core.MetaInfo) (*Torrent, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	tc := metainfoclient.NewTestClient()

	ta := DefaultTorrentArchive(tally.NoopScope, fs, tc)

	if err := tc.Upload(mi); err != nil {
		panic(err)
	}

	t, err := ta.CreateTorrent("noexist", mi.Name())
	if err != nil {
		panic(err)
	}

	return t.(*Torrent), cleanup.Run
}
