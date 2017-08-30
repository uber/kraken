package scheduler

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/client/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/testutil"
)

const testTempDir = "/tmp/kraken_scheduler"

func init() {
	os.Mkdir(testTempDir, 0755)
}

func genConfig(trackerAddr string) Config {
	return Config{
		ListenAddr:       "localhost:0",
		Datacenter:       "sjc1",
		TrackerAddr:      trackerAddr,
		AnnounceInterval: 500 * time.Millisecond,
		// Buffers are just a performance optimization, so a zero-sized
		// buffer will instantly force any deadlock conditions.
		SenderBufferSize:           0,
		ReceiverBufferSize:         0,
		IdleSeederTTL:              2 * time.Second,
		PreemptionInterval:         500 * time.Millisecond,
		IdleConnTTL:                1 * time.Second,
		ConnTTL:                    5 * time.Minute,
		InitialBlacklistExpiration: time.Second,
		BlacklistExpirationBackoff: 2,
		MaxBlacklistExpiration:     10 * time.Second,
		ExpiredBlacklistEntryTTL:   5 * time.Minute,
		BlacklistCleanupInterval:   time.Minute,
	}
}

// writeTorrent writes the given content into a torrent file into tm's storage.
// Useful for populating a completed torrent before seeding it.
func writeTorrent(ta storage.TorrentArchive, mi *torlib.MetaInfo, content []byte) storage.Torrent {
	t, err := ta.CreateTorrent(mi.InfoHash, mi)
	if err != nil {
		panic(err)
	}

	for i := 0; i < t.NumPieces(); i++ {
		start := int64(i) * mi.Info.PieceLength
		end := start + t.PieceLength(i)
		if _, err := t.WritePiece(content[start:end], i); err != nil {
			panic(err)
		}
	}
	return t
}

func checkContent(r *require.Assertions, t storage.Torrent, expected []byte) {
	result := make([]byte, t.Length())
	cursor := result
	for i := 0; i < t.NumPieces(); i++ {
		pieceData, err := t.ReadPiece(i)
		r.NoError(err)
		copy(cursor, pieceData)
		cursor = cursor[t.PieceLength(i):]
	}
	r.Equal(expected, result)
}

type testPeer struct {
	Scheduler      *Scheduler
	TorrentArchive storage.TorrentArchive
	Stop           func()
}

func genTestPeer(config Config) *testPeer {
	tm, deleteFunc := storage.TorrentArchiveFixture()
	s, err := New(config, torlib.PeerIDFixture(), tm)
	if err != nil {
		deleteFunc()
		panic(err)
	}

	stop := func() {
		s.Stop()
		deleteFunc()
	}
	return &testPeer{s, tm, stop}
}

func genTestPeers(n int, config Config) (peers []*testPeer, stopAll func()) {
	peers = make([]*testPeer, n)
	for i := range peers {
		peers[i] = genTestPeer(config)
	}
	return peers, func() {
		for _, p := range peers {
			p.Stop()
		}
	}
}

type hasConnEvent struct {
	peerID   torlib.PeerID
	infoHash torlib.InfoHash
	result   chan bool
}

func (e hasConnEvent) Apply(s *Scheduler) {
	_, ok := s.connState.active[connKey{e.peerID, e.infoHash}]
	e.result <- ok
}

// waitForConnEstablished waits until s has established a connection to peerID for the
// torrent of infoHash.
func waitForConnEstablished(t *testing.T, s *Scheduler, peerID torlib.PeerID, infoHash torlib.InfoHash) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		result := make(chan bool)
		s.eventLoop.Send(hasConnEvent{peerID, infoHash, result})
		return <-result
	})
	if err != nil {
		t.Fatalf(
			"scheduler=%s did not establish conn to peer=%s hash=%s: %s",
			s.peerID, peerID, infoHash, err)
	}
}

// waitForConnRemoved waits until s has closed the connection to peerID for the
// torrent of infoHash.
func waitForConnRemoved(t *testing.T, s *Scheduler, peerID torlib.PeerID, infoHash torlib.InfoHash) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		result := make(chan bool)
		s.eventLoop.Send(hasConnEvent{peerID, infoHash, result})
		return !<-result
	})
	if err != nil {
		t.Fatalf(
			"scheduler=%s did not remove conn to peer=%s hash=%s: %s",
			s.peerID, peerID, infoHash, err)
	}
}

// hasConn checks whether s has an established connection to peerID for the
// torrent of infoHash.
func hasConn(s *Scheduler, peerID torlib.PeerID, infoHash torlib.InfoHash) bool {
	result := make(chan bool)
	s.eventLoop.Send(hasConnEvent{peerID, infoHash, result})
	return <-result
}

type hasTorrentEvent struct {
	infoHash torlib.InfoHash
	result   chan bool
}

func (e hasTorrentEvent) Apply(s *Scheduler) {
	_, ok := s.torrentControls[e.infoHash]
	e.result <- ok
}

func waitForTorrentRemoved(t *testing.T, s *Scheduler, infoHash torlib.InfoHash) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		result := make(chan bool)
		s.eventLoop.Send(hasTorrentEvent{infoHash, result})
		return !<-result
	})
	if err != nil {
		t.Fatalf(
			"scheduler=%s did not remove torrent for hash=%s: %s",
			s.peerID, infoHash, err)
	}
}
