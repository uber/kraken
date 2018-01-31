package storage

import (
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/willf/bitset"
)

// TorrentArchiveFixture creates a new TorrentArchive and returns the archive with a cleanup function
func TorrentArchiveFixture() (TorrentArchive, func()) {
	localStore, cleanup := store.LocalFileStoreFixture()
	archive := NewAgentTorrentArchive(
		AgentTorrentArchiveConfig{}, localStore, nil)
	return archive, cleanup
}

// TorrentFixture returns a Torrent for the given size and piece length.
func TorrentFixture(size, pieceLength uint64) (Torrent, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	tc := metainfoclient.NewTestClient()

	ta := NewAgentTorrentArchive(AgentTorrentArchiveConfig{}, fs, tc)

	mi := torlib.CustomMetaInfoFixture(size, pieceLength)
	if err := tc.Upload(mi); err != nil {
		panic(err)
	}
	tor, err := ta.CreateTorrent("noexist", mi.Name())
	if err != nil {
		panic(err)
	}

	return tor, cleanup.Run
}

// TorrentInfoFixture returns a TorrentInfo for the given size and piece length.
func TorrentInfoFixture(size, pieceLength uint64) (*TorrentInfo, func()) {
	torrent, cleanup := TorrentFixture(size, pieceLength)
	return torrent.Stat(), cleanup
}

// BitSetFixture returns a new BitSet with the give bits set.
func BitSetFixture(bs ...bool) *bitset.BitSet {
	s := bitset.New(uint(len(bs)))
	for i, b := range bs {
		s.SetTo(uint(i), b)
	}
	return s
}
