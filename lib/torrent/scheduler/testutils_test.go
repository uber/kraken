package scheduler

import (
	"flag"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/announcequeue"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/connstate"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/dispatch"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/torrentlog"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/lib/torrent/storage/agentstorage"
	"code.uber.internal/infra/kraken/lib/torrent/storage/piecereader"
	"code.uber.internal/infra/kraken/mocks/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/tracker/trackerserver"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/testutil"
)

const testTempDir = "/tmp/kraken_scheduler"

const namespace = "test-namespace"

func init() {
	os.Mkdir(testTempDir, 0755)

	debug := flag.Bool("scheduler.debug", false, "log all Scheduler debugging output")
	flag.Parse()

	zapConfig := zap.NewProductionConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	zapConfig.Encoding = "console"

	if !*debug {
		zapConfig.OutputPaths = []string{}
	}

	log.ConfigureLogger(zapConfig)
}

func configFixture() Config {
	return Config{
		SeederTTI:          10 * time.Second,
		LeecherTTI:         time.Minute,
		PreemptionInterval: 500 * time.Millisecond,
		ConnTTI:            10 * time.Second,
		ConnTTL:            5 * time.Minute,
		ConnState:          connstate.Config{},
		Conn:               conn.ConfigFixture(),
		Dispatch:           dispatch.Config{},
		TorrentLog:         torrentlog.Config{Disable: true},
	}.applyDefaults()
}

type testMocks struct {
	ctrl           *gomock.Controller
	metaInfoClient *mockmetainfoclient.MockClient
	trackerAddr    string
	cleanup        *testutil.Cleanup
}

func newTestMocks(t gomock.TestReporter) (*testMocks, func()) {
	var cleanup testutil.Cleanup

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	trackerAddr, stop := testutil.StartServer(trackerserver.Fixture().Handler())
	cleanup.Add(stop)

	return &testMocks{
		ctrl:           ctrl,
		metaInfoClient: mockmetainfoclient.NewMockClient(ctrl),
		trackerAddr:    trackerAddr,
		cleanup:        &cleanup,
	}, cleanup.Run
}

type testPeer struct {
	pctx           core.PeerContext
	scheduler      *scheduler
	torrentArchive storage.TorrentArchive
	stats          tally.TestScope
	testProducer   *networkevent.TestProducer
	fs             store.FileStore
	cleanup        *testutil.Cleanup
}

func (m *testMocks) newPeer(config Config, options ...option) *testPeer {
	var cleanup testutil.Cleanup
	m.cleanup.Add(cleanup.Run)

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	stats := tally.NewTestScope("", nil)

	ta := agentstorage.NewTorrentArchive(stats, fs, m.metaInfoClient)

	pctx := core.PeerContext{
		PeerID: core.PeerIDFixture(),
		Zone:   "sjc1",
		IP:     "localhost",
		Port:   findFreePort(),
	}
	ac := announceclient.New(pctx, m.trackerAddr)
	tp := networkevent.NewTestProducer()

	s, err := newScheduler(config, ta, stats, pctx, ac, announcequeue.New(), tp, options...)
	if err != nil {
		panic(err)
	}
	cleanup.Add(s.Stop)

	return &testPeer{pctx, s, ta, stats, tp, fs, &cleanup}
}

func (m *testMocks) newPeers(n int, config Config) []*testPeer {
	var peers []*testPeer
	for i := 0; i < n; i++ {
		peers = append(peers, m.newPeer(config))
	}
	return peers
}

// writeTorrent writes the given content into a torrent file into peers storage.
// Useful for populating a completed torrent before seeding it.
func (p *testPeer) writeTorrent(blob *core.BlobFixture) {
	t, err := p.torrentArchive.CreateTorrent(namespace, blob.MetaInfo.Name())
	if err != nil {
		panic(err)
	}
	for i := 0; i < t.NumPieces(); i++ {
		start := int64(i) * blob.MetaInfo.Info.PieceLength
		end := start + t.PieceLength(i)
		if err := t.WritePiece(piecereader.NewBuffer(blob.Content[start:end]), i); err != nil {
			panic(err)
		}
	}
}

func (p *testPeer) checkTorrent(t *testing.T, blob *core.BlobFixture) {
	require := require.New(t)

	tor, err := p.torrentArchive.GetTorrent(namespace, blob.MetaInfo.Info.Name)
	require.NoError(err)

	require.True(tor.Complete())

	result := make([]byte, tor.Length())
	cursor := result
	for i := 0; i < tor.NumPieces(); i++ {
		pr, err := tor.GetPieceReader(i)
		require.NoError(err)
		defer pr.Close()
		pieceData, err := ioutil.ReadAll(pr)
		require.NoError(err)
		copy(cursor, pieceData)
		cursor = cursor[tor.PieceLength(i):]
	}
	require.Equal(blob.Content, result)
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

type hasConnEvent struct {
	peerID   core.PeerID
	infoHash core.InfoHash
	result   chan bool
}

func (e hasConnEvent) Apply(s *scheduler) {
	found := false
	conns := s.connState.ActiveConns()
	for _, c := range conns {
		if c.PeerID() == e.peerID && c.InfoHash() == e.infoHash {
			found = true
			break
		}
	}
	e.result <- found
}

// waitForConnEstablished waits until s has established a connection to peerID for the
// torrent of infoHash.
func waitForConnEstablished(t *testing.T, s *scheduler, peerID core.PeerID, infoHash core.InfoHash) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		result := make(chan bool)
		s.eventLoop.Send(hasConnEvent{peerID, infoHash, result})
		return <-result
	})
	if err != nil {
		t.Fatalf(
			"scheduler=%s did not establish conn to peer=%s hash=%s: %s",
			s.pctx.PeerID, peerID, infoHash, err)
	}
}

// waitForConnRemoved waits until s has closed the connection to peerID for the
// torrent of infoHash.
func waitForConnRemoved(t *testing.T, s *scheduler, peerID core.PeerID, infoHash core.InfoHash) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		result := make(chan bool)
		s.eventLoop.Send(hasConnEvent{peerID, infoHash, result})
		return !<-result
	})
	if err != nil {
		t.Fatalf(
			"scheduler=%s did not remove conn to peer=%s hash=%s: %s",
			s.pctx.PeerID, peerID, infoHash, err)
	}
}

// hasConn checks whether s has an established connection to peerID for the
// torrent of infoHash.
func hasConn(s *scheduler, peerID core.PeerID, infoHash core.InfoHash) bool {
	result := make(chan bool)
	s.eventLoop.Send(hasConnEvent{peerID, infoHash, result})
	return <-result
}

type hasTorrentEvent struct {
	infoHash core.InfoHash
	result   chan bool
}

func (e hasTorrentEvent) Apply(s *scheduler) {
	_, ok := s.torrentControls[e.infoHash]
	e.result <- ok
}

func waitForTorrentRemoved(t *testing.T, s *scheduler, infoHash core.InfoHash) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		result := make(chan bool)
		s.eventLoop.Send(hasTorrentEvent{infoHash, result})
		return !<-result
	})
	if err != nil {
		t.Fatalf(
			"scheduler=%s did not remove torrent for hash=%s: %s",
			s.pctx.PeerID, infoHash, err)
	}
}

func waitForTorrentAdded(t *testing.T, s *scheduler, infoHash core.InfoHash) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		result := make(chan bool)
		s.eventLoop.Send(hasTorrentEvent{infoHash, result})
		return <-result
	})
	if err != nil {
		t.Fatalf(
			"scheduler=%s did not add torrent for hash=%s: %s",
			s.pctx.PeerID, infoHash, err)
	}
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
	name := reflect.TypeOf(e).Name()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case ee := <-w.events:
			if name == reflect.TypeOf(ee).Name() {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for %s to occur", name)
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

func (w *eventWatcher) SendTimeout(e event, timeout time.Duration) error {
	panic("unimplemented")
}

func (w *eventWatcher) Run(s *scheduler) {
	w.l.Run(s)
}

func (w *eventWatcher) Stop() {
	w.l.Stop()
}
