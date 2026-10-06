// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scheduler

import (
	"errors"
	"io"
	"net"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler/announcequeue"
	"github.com/uber/kraken/lib/torrent/scheduler/conn"
	"github.com/uber/kraken/lib/torrent/scheduler/connstate"
	"github.com/uber/kraken/lib/torrent/scheduler/dispatch"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/lib/torrent/storage/agentstorage"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"
	mockmetainfoclient "github.com/uber/kraken/mocks/tracker/metainfoclient"
	"github.com/uber/kraken/tracker/announceclient"
	"github.com/uber/kraken/tracker/trackerserver"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/testutil"
)

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
		TorrentLog:         log.Config{Disable: true},
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
	cads           *store.CADownloadStore
	cleanup        *testutil.Cleanup
}

// _maxPeerBindAttempts bounds how many ports newPeer draws before it gives up
// on finding one the scheduler can still bind.
const _maxPeerBindAttempts = 5

func (m *testMocks) newPeer(config Config, options ...option) *testPeer {
	var cleanup testutil.Cleanup
	m.cleanup.Add(cleanup.Run)

	cads, c := store.CADownloadStoreFixture()
	cleanup.Add(c)

	stats := tally.NewTestScope("", nil)

	ta := agentstorage.NewTorrentArchive(stats, cads, m.metaInfoClient)

	tp := networkevent.NewTestProducer()

	// findFreePort must release a port before the scheduler can bind it, so
	// another listener may still claim it in between. Re-draw a port and retry
	// instead of panicking the whole test binary. start returns before it adds
	// to the wait group or launches a goroutine, so a scheduler which failed to
	// bind holds nothing and needs no cleanup.
	var pctx core.PeerContext
	var s *scheduler
	for attempt := 1; ; attempt++ {
		pctx = core.PeerContext{
			PeerID: core.PeerIDFixture(),
			Zone:   "zone1",
			IP:     "localhost",
			Port:   findFreePort(),
		}
		ac := announceclient.New(pctx, hashring.NoopPassiveRing(hostlist.Fixture(m.trackerAddr)), nil)

		var err error
		s, err = newScheduler(config, ta, stats, pctx, ac, tp, options...)
		if err != nil {
			panic(err)
		}
		err = s.start(announcequeue.New())
		if err == nil {
			break
		}
		if attempt == _maxPeerBindAttempts || !errors.Is(err, syscall.EADDRINUSE) {
			panic(err)
		}
	}
	cleanup.Add(s.Stop)

	return &testPeer{pctx, s, ta, stats, tp, cads, &cleanup}
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
func (p *testPeer) writeTorrent(namespace string, blob *core.BlobFixture) {
	t, err := p.torrentArchive.CreateTorrent(namespace, blob.Digest)
	if err != nil {
		panic(err)
	}
	for i := 0; i < t.NumPieces(); i++ {
		start := int64(i) * blob.MetaInfo.PieceLength()
		end := start + t.PieceLength(i)
		if err := t.WritePiece(piecereader.NewBuffer(blob.Content[start:end]), i); err != nil {
			panic(err)
		}
	}
}

func (p *testPeer) checkTorrent(t *testing.T, namespace string, blob *core.BlobFixture) {
	require := require.New(t)

	tor, err := p.torrentArchive.GetTorrent(namespace, blob.Digest)
	require.NoError(err)

	require.True(tor.Complete())

	result := make([]byte, tor.Length())
	cursor := result
	for i := 0; i < tor.NumPieces(); i++ {
		pr, err := tor.GetPieceReader(i)
		require.NoError(err)
		pieceData, err := io.ReadAll(pr)
		require.NoError(err)
		require.NoError(pr.Close())
		copy(cursor, pieceData)
		cursor = cursor[tor.PieceLength(i):]
	}
	require.Equal(blob.Content, result)
}

// findFreePort returns a port which is free for a wildcard bind.
//
// The probe below must bind the same address form the caller binds. A
// scheduler binds the wildcard ":%d" (see scheduler.start), and the kernel
// lets a loopback bind and a wildcard bind share one port. Probing
// "localhost:0" therefore hands back ports which a live peer already listens
// on, and the next scheduler.start panics with "address already in use".
func findFreePort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			panic(err)
		}
	}()
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

func (e hasConnEvent) apply(s *state) {
	found := false
	conns := s.conns.ActiveConns()
	for _, c := range conns {
		if c.PeerID() == e.peerID && c.InfoHash() == e.infoHash {
			found = true
			break
		}
	}
	e.result <- found
}

// waitForConnRemoved waits until s has closed the connection to peerID for the
// torrent of infoHash.
func waitForConnRemoved(t *testing.T, s *scheduler, peerID core.PeerID, infoHash core.InfoHash) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		result := make(chan bool)
		s.eventLoop.send(hasConnEvent{peerID, infoHash, result})
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
	s.eventLoop.send(hasConnEvent{peerID, infoHash, result})
	return <-result
}

type hasTorrentEvent struct {
	infoHash core.InfoHash
	result   chan bool
}

func (e hasTorrentEvent) apply(s *state) {
	_, ok := s.torrentControls[e.infoHash]
	e.result <- ok
}

func waitForTorrentRemoved(t *testing.T, s *scheduler, infoHash core.InfoHash) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		result := make(chan bool)
		s.eventLoop.send(hasTorrentEvent{infoHash, result})
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
		s.eventLoop.send(hasTorrentEvent{infoHash, result})
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

// waitFor waits for e to send on w.
func (w *eventWatcher) waitFor(t *testing.T, e event) {
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

func (w *eventWatcher) send(e event) bool {
	if w.l.send(e) {
		go func() { w.events <- e }()
		return true
	}
	return false
}

func (w *eventWatcher) sendTimeout(e event, timeout time.Duration) error {
	panic("unimplemented")
}

func (w *eventWatcher) run(s *state) {
	w.l.run(s)
}

func (w *eventWatcher) stop() {
	w.l.stop()
}
