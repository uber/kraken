package storage

import "code.uber.internal/infra/kraken/utils"

// All functions defined in this file are intended for testing purposes only.
// TODO(codyg): Consider mandating blackbox testing for all tests, and moving
// fixtures into their own package.

// InfoHashFixture creates a random InfoHash.
func InfoHashFixture() string {
	return utils.RandomHexString(64)
}

// PeerIDFixture creates a random PeerID.
func PeerIDFixture() string {
	return utils.RandomHexString(64)
}

// TorrentFixture creates a new TorrentInfo.
func TorrentFixture() *TorrentInfo {
	return &TorrentInfo{
		TorrentName: "torrent",
		InfoHash:    InfoHashFixture(),
		Author:      "a guy",
		NumPieces:   123,
		PieceLength: 20000,
		RefCount:    1,
		Flags:       0,
	}
}

// PeerFixture creates a new PeerInfo.
func PeerFixture() *PeerInfo {
	return &PeerInfo{
		InfoHash:        InfoHashFixture(),
		PeerID:          PeerIDFixture(),
		IP:              "192.168.1.1",
		Priority:        0,
		DC:              "sjc1",
		Port:            6881,
		BytesUploaded:   5678,
		BytesDownloaded: 1234,
		BytesLeft:       910,
		Event:           "stopped",
		Flags:           0,
	}
}

// PeerForTorrentFixture creates a PeerInfo which derives from the given TorrentInfo.
func PeerForTorrentFixture(t *TorrentInfo) *PeerInfo {
	p := PeerFixture()
	p.InfoHash = t.InfoHash
	return p
}
