package agentstorage

import (
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/tracker/metainfoclient"
	"github.com/uber/kraken/utils/testutil"
	"github.com/uber-go/tally"
)

// TorrentArchiveFixture returns a TorrrentArchive for testing purposes.
func TorrentArchiveFixture() (*TorrentArchive, func()) {
	cads, cleanup := store.CADownloadStoreFixture()
	archive := NewTorrentArchive(tally.NoopScope, cads, nil)
	return archive, cleanup
}

// TorrentFixture returns a Torrent for the given metainfo for testing purposes.
func TorrentFixture(mi *core.MetaInfo) (*Torrent, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	cads, c := store.CADownloadStoreFixture()
	cleanup.Add(c)

	tc := metainfoclient.NewTestClient()

	ta := NewTorrentArchive(tally.NoopScope, cads, tc)

	if err := tc.Upload(mi); err != nil {
		panic(err)
	}

	t, err := ta.CreateTorrent("noexist", mi.Digest())
	if err != nil {
		panic(err)
	}

	return t.(*Torrent), cleanup.Run
}
