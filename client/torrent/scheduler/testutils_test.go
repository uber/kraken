package scheduler

import (
	"io"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/client/torrent/storage"
	"code.uber.internal/infra/kraken/mocks/client/torrent/mockstorage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/testutil"
)

const testTempDir = "/tmp/kraken_scheduler"

func init() {
	os.Mkdir(testTempDir, 0755)
}

func genConnConfig() ConnConfig {
	return ConnConfig{
		// Buffers are just a performance optimization, so a zero-sized
		// buffer will instantly force any deadlock conditions.
		SenderBufferSize:   0,
		ReceiverBufferSize: 0,
	}.applyDefaults()
}

func genConnStateConfig() ConnStateConfig {
	return ConnStateConfig{
		MaxOpenConnectionsPerTorrent: 20,
		InitialBlacklistExpiration:   time.Second,
		BlacklistExpirationBackoff:   2,
		MaxBlacklistExpiration:       10 * time.Second,
		ExpiredBlacklistEntryTTL:     5 * time.Minute,
	}.applyDefaults()
}

func genConfig(trackerAddr string) Config {
	c, err := Config{
		ListenAddr:               "localhost:0",
		Datacenter:               "sjc1",
		TrackerAddr:              trackerAddr,
		AnnounceInterval:         500 * time.Millisecond,
		IdleSeederTTL:            2 * time.Second,
		PreemptionInterval:       500 * time.Millisecond,
		IdleConnTTL:              1 * time.Second,
		ConnTTL:                  5 * time.Minute,
		BlacklistCleanupInterval: time.Minute,
		ConnState:                genConnStateConfig(),
		Conn:                     genConnConfig(),
	}.applyDefaults()
	if err != nil {
		panic(err)
	}
	return c
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

func genTestPeer(config Config, options ...option) *testPeer {
	tm, deleteFunc := storage.TorrentArchiveFixture()
	s, err := New(config, torlib.PeerIDFixture(), tm, options...)
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

func discard(nc net.Conn) {
	for {
		if _, err := io.Copy(ioutil.Discard, nc); err != nil {
			return
		}
	}
}

type noopEventSender struct{}

func (s noopEventSender) Send(event) {}

func genTestConn(t *testing.T, config ConnConfig, maxPieceLength int) (c *conn, cleanup func()) {
	ctrl := gomock.NewController(t)

	infoHash := torlib.InfoHashFixture()
	localPeerID := torlib.PeerIDFixture()
	remotePeerID := torlib.PeerIDFixture()

	f := &connFactory{
		Config:      config,
		LocalPeerID: localPeerID,
		EventSender: noopEventSender{},
		Clock:       clock.New(),
	}

	localNC, remoteNC := net.Pipe()
	go discard(remoteNC)

	tor := mockstorage.NewMockTorrent(ctrl)
	tor.EXPECT().InfoHash().Return(infoHash)
	tor.EXPECT().MaxPieceLength().Return(int64(maxPieceLength))

	c = f.newConn(localNC, tor, remotePeerID, storage.Bitfield{}, false)
	cleanup = func() {
		localNC.Close()
		remoteNC.Close()
	}
	return
}

// eventRecorder wraps an eventLoop and records all events being sent.
type eventRecorder struct {
	done   chan struct{}
	l      eventLoop
	mu     sync.Mutex
	events []event
}

func newEventRecorder() *eventRecorder {
	done := make(chan struct{})
	return &eventRecorder{
		done: done,
		l:    newEventLoop(done),
	}
}

func (r *eventRecorder) Send(e event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.l.Send(e)
	r.mu.Unlock()
}

func (r *eventRecorder) Run(s *Scheduler) {
	r.l.Run(s)
}

func (r *eventRecorder) Events() []event {
	r.mu.Lock()
	a := make([]event, len(r.events))
	copy(a, r.events)
	r.mu.Unlock()
	return a
}

func (r *eventRecorder) Stop() {
	close(r.done)
}
