package scheduler

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/mocks/lib/torrent/mockstorage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/testutil"
)

const testTempDir = "/tmp/kraken_scheduler"

func init() {
	os.Mkdir(testTempDir, 0755)

	debug := flag.Bool("scheduler.debug", false, "log all Scheduler debugging output")
	flag.Parse()

	formatter := true
	logConfig := &log.Configuration{
		Level:         log.DebugLevel,
		Stdout:        *debug,
		TextFormatter: &formatter,
	}
	log.Configure(logConfig, true)
}

func connConfigFixture() ConnConfig {
	return ConnConfig{
		// Buffers are just a performance optimization, so a zero-sized
		// buffer will instantly force any deadlock conditions.
		SenderBufferSize:   0,
		ReceiverBufferSize: 0,
	}.applyDefaults()
}

func connStateConfigFixture() ConnStateConfig {
	return ConnStateConfig{
		MaxOpenConnectionsPerTorrent: 20,
		InitialBlacklistExpiration:   250 * time.Millisecond,
		BlacklistExpirationBackoff:   1,
		MaxBlacklistExpiration:       1 * time.Second,
		ExpiredBlacklistEntryTTL:     5 * time.Minute,
	}.applyDefaults()
}

func configFixture(trackerAddr string) Config {
	c, err := Config{
		ListenAddr:               "localhost:0",
		TrackerAddr:              trackerAddr,
		AnnounceInterval:         500 * time.Millisecond,
		IdleSeederTTL:            10 * time.Second,
		PreemptionInterval:       500 * time.Millisecond,
		IdleConnTTL:              10 * time.Second,
		ConnTTL:                  5 * time.Minute,
		BlacklistCleanupInterval: time.Minute,
		ConnState:                connStateConfigFixture(),
		Conn:                     connConfigFixture(),
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
	Stats          tally.TestScope
	Stop           func()
}

func findFreePort() int {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	_, portStr, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		panic(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(err)
	}
	return port
}

func testPeerFixture(config Config, options ...option) *testPeer {
	tm, cleanup := storage.TorrentArchiveFixture()
	stats := tally.NewTestScope("", nil)
	pctx := peercontext.PeerContext{
		PeerID: torlib.PeerIDFixture(),
		Zone:   "sjc1",
		IP:     "localhost",
		Port:   findFreePort(),
	}
	config.ListenAddr = fmt.Sprintf("%s:%d", pctx.IP, pctx.Port)
	s, err := New(config, tm, stats, pctx, options...)
	if err != nil {
		cleanup()
		panic(err)
	}
	stop := func() {
		s.Stop()
		cleanup()
	}
	return &testPeer{s, tm, stats, stop}
}

func testPeerFixtures(n int, config Config) (peers []*testPeer, stopAll func()) {
	peers = make([]*testPeer, n)
	for i := range peers {
		peers[i] = testPeerFixture(config)
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

func (s noopEventSender) Send(event) bool { return true }

// noopDeadline wraps a Conn which does not support deadlines (e.g. net.Pipe)
// and makes it accept deadlines.
type noopDeadline struct {
	net.Conn
}

func (n noopDeadline) SetDeadline(t time.Time) error      { return nil }
func (n noopDeadline) SetReadDeadline(t time.Time) error  { return nil }
func (n noopDeadline) SetWriteDeadline(t time.Time) error { return nil }

func connFixture(t *testing.T, config ConnConfig, maxPieceLength int) (c *conn, cleanup func()) {
	ctrl := gomock.NewController(t)

	infoHash := torlib.InfoHashFixture()
	localPeerID := torlib.PeerIDFixture()
	remotePeerID := torlib.PeerIDFixture()

	f := &connFactory{
		Config:      config,
		LocalPeerID: localPeerID,
		EventSender: noopEventSender{},
		Clock:       clock.New(),
		Stats:       tally.NewTestScope("", nil),
	}

	localNC, remoteNC := net.Pipe()
	localNC = noopDeadline{localNC}
	go discard(remoteNC)

	tor := mockstorage.NewMockTorrent(ctrl)
	tor.EXPECT().Name().Return("some dummy name")
	tor.EXPECT().InfoHash().Return(infoHash)
	tor.EXPECT().MaxPieceLength().Return(int64(maxPieceLength))

	c = f.newConn(localNC, tor, remotePeerID, storage.Bitfield{}, false)
	cleanup = func() {
		localNC.Close()
		remoteNC.Close()
	}
	return
}

// eventWatcher wraps an eventLoop and watches all events being sent. Note, clients
// must call WaitFor else all sends will block.
type eventWatcher struct {
	l      eventLoop
	events chan event
}

func newEventWatcher() *eventWatcher {
	return &eventWatcher{
		l:      newEventLoop(),
		events: make(chan event),
	}
}

// WaitFor waits for e to send on w.
func (w *eventWatcher) WaitFor(t *testing.T, e event) {
	timeout := time.After(5 * time.Second)
	for {
		select {
		case ee := <-w.events:
			if reflect.DeepEqual(e, ee) {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for %s to occur", reflect.TypeOf(e).Name())
		}
	}
}

func (w *eventWatcher) Send(e event) bool {
	if w.l.Send(e) {
		go func() { w.events <- e }()
		return true
	}
	return false
}

func (w *eventWatcher) Run(s *Scheduler) {
	w.l.Run(s)
}

func (w *eventWatcher) Stop() {
	w.l.Stop()
}
