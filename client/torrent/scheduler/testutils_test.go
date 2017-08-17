package scheduler

import (
	"io/ioutil"
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
		TrackerAddr:                  trackerAddr,
		MaxOpenConnectionsPerTorrent: 20,
		AnnounceInterval:             500 * time.Millisecond,
		DialTimeout:                  5 * time.Second,
		WriteTimeout:                 5 * time.Second,
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

type tempTorrentManager struct {
	storage.TorrentManager
	tmpdir string
}

func (m *tempTorrentManager) Delete() {
	if err := os.RemoveAll(m.tmpdir); err != nil {
		panic(err)
	}
}

// TODO(codyg): Move this to storage package.
func genTorrentManager() *tempTorrentManager {
	d, err := ioutil.TempDir(testTempDir, "manager_")
	if err != nil {
		panic(err)
	}
	return &tempTorrentManager{storage.NewFileStorage(d), d}
}

// writeTorrent writes the given content into a torrent file into tm's storage.
// Useful for populating a completed torrent before seeding it.
func writeTorrent(tm storage.TorrentManager, mi *torlib.MetaInfo, content []byte) storage.Torrent {
	infoByte, err := mi.Info.Serialize()
	if err != nil {
		panic(err)
	}
	t, err := tm.CreateTorrent(mi.GetInfoHash(), infoByte)
	if err != nil {
		panic(err)
	}
	if _, err := t.WriteAt(content, 0); err != nil {
		panic(err)
	}
	return t
}

type testPeer struct {
	Scheduler      *Scheduler
	TorrentManager *tempTorrentManager
}

func (p *testPeer) Stop() {
	p.Scheduler.Stop()
	p.TorrentManager.Delete()
}

func genTestPeer(config Config) *testPeer {
	tm := genTorrentManager()
	s, err := New(torlib.PeerIDFixture(), "localhost:0", "sjc1", tm, config)
	if err != nil {
		tm.Delete()
		panic(err)
	}
	return &testPeer{s, tm}
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

func checkContent(r *require.Assertions, t storage.Torrent, expected []byte) {
	result := make([]byte, len(expected))
	_, err := t.ReadAt(result, 0)
	r.NoError(err)
	r.Equal(expected, result)
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

type hasDispatcherEvent struct {
	infoHash torlib.InfoHash
	result   chan bool
}

func (e hasDispatcherEvent) Apply(s *Scheduler) {
	_, ok := s.dispatchers[e.infoHash]
	e.result <- ok
}

func waitForDispatcherRemoved(t *testing.T, s *Scheduler, infoHash torlib.InfoHash) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		result := make(chan bool)
		s.eventLoop.Send(hasDispatcherEvent{infoHash, result})
		return !<-result
	})
	if err != nil {
		t.Fatalf(
			"scheduler=%s did not remove dispatcher for hash=%s: %s",
			s.peerID, infoHash, err)
	}
}
