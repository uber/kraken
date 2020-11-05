// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scheduler

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler/announcequeue"
	"github.com/uber/kraken/lib/torrent/scheduler/conn"
	"github.com/uber/kraken/lib/torrent/scheduler/connstate"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/lib/torrent/storage/agentstorage"
	mockannounceclient "github.com/uber/kraken/mocks/tracker/announceclient"
	mockmetainfoclient "github.com/uber/kraken/mocks/tracker/metainfoclient"
	"github.com/uber/kraken/tracker/announceclient"
	"github.com/uber/kraken/utils/testutil"
)

const _testNamespace = "noexist"

type mockEventLoop struct {
	t *testing.T
	c chan event
}

func (l *mockEventLoop) expect(e event) {
	select {
	case result := <-l.c:
		require.Equal(l.t, e, result)
	case <-time.After(5 * time.Second):
		l.t.Fatalf("timed out waiting for %T to occur", e)
	}
}

func (l *mockEventLoop) send(e event) bool {
	l.c <- e
	return true
}

// Unimplemented.
func (l *mockEventLoop) run(*state)                                       {}
func (l *mockEventLoop) stop()                                            {}
func (l *mockEventLoop) sendTimeout(e event, timeout time.Duration) error { panic("unimplemented") }

type stateMocks struct {
	metainfoClient *mockmetainfoclient.MockClient
	announceClient *mockannounceclient.MockClient
	announceQueue  announcequeue.Queue
	torrentArchive storage.TorrentArchive
	eventLoop      *mockEventLoop
}

func newStateMocks(t *testing.T) (*stateMocks, func()) {
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	metainfoClient := mockmetainfoclient.NewMockClient(ctrl)

	announceClient := mockannounceclient.NewMockClient(ctrl)

	cads, c := store.CADownloadStoreFixture()
	cleanup.Add(c)

	mocks := &stateMocks{
		metainfoClient: metainfoClient,
		announceClient: announceClient,
		announceQueue:  announcequeue.New(),
		torrentArchive: agentstorage.NewTorrentArchive(tally.NoopScope, cads, metainfoClient),
		eventLoop:      &mockEventLoop{t, make(chan event)},
	}
	return mocks, cleanup.Run
}

func (m *stateMocks) newState(config Config) *state {
	sched, err := newScheduler(
		config,
		m.torrentArchive,
		tally.NoopScope,
		core.PeerContextFixture(),
		m.announceClient,
		networkevent.NewTestProducer(),
		withEventLoop(m.eventLoop))
	if err != nil {
		panic(err)
	}
	return newState(sched, m.announceQueue)
}

func (m *stateMocks) newTorrent() storage.Torrent {
	mi := core.MetaInfoFixture()

	m.metainfoClient.EXPECT().
		Download(_testNamespace, mi.Digest()).
		Return(mi, nil)

	t, err := m.torrentArchive.CreateTorrent(_testNamespace, mi.Digest())
	if err != nil {
		panic(err)
	}
	return t
}

func TestAnnounceTickEvent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStateMocks(t)
	defer cleanup()

	state := mocks.newState(Config{})

	var ctrls []*torrentControl
	for i := 0; i < 5; i++ {
		c, err := state.addTorrent(_testNamespace, mocks.newTorrent(), true)
		require.NoError(err)
		ctrls = append(ctrls, c)
	}

	// First torrent should announce.
	mocks.announceClient.EXPECT().
		Announce(
			ctrls[0].dispatcher.Digest(),
			ctrls[0].dispatcher.InfoHash(),
			false,
			announceclient.V2).
		Return(nil, time.Second, nil)

	announceTickEvent{}.apply(state)

	mocks.eventLoop.expect(announceResultEvent{
		infoHash: ctrls[0].dispatcher.InfoHash(),
	})
}

func TestAnnounceTickEventSkipsFullTorrents(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStateMocks(t)
	defer cleanup()

	state := mocks.newState(Config{
		ConnState: connstate.Config{
			MaxOpenConnectionsPerTorrent: 5,
		},
	})

	full, err := state.addTorrent(_testNamespace, mocks.newTorrent(), true)
	require.NoError(err)

	info := full.dispatcher.Stat()

	for i := 0; i < 5; i++ {
		_, c, cleanup := conn.PipeFixture(conn.Config{}, info)
		defer cleanup()

		require.NoError(state.conns.AddPending(c.PeerID(), c.InfoHash(), nil))
		require.NoError(state.addOutgoingConn(c, info.Bitfield(), info))
	}

	empty, err := state.addTorrent(_testNamespace, mocks.newTorrent(), true)
	require.NoError(err)

	// The first torrent is full and should be skipped, announcing the empty
	// torrent.
	mocks.announceClient.EXPECT().
		Announce(
			empty.dispatcher.Digest(),
			empty.dispatcher.InfoHash(),
			false,
			announceclient.V2).
		Return(nil, time.Second, nil)

	announceTickEvent{}.apply(state)

	// Empty torrent announced.
	mocks.eventLoop.expect(announceResultEvent{
		infoHash: empty.dispatcher.InfoHash(),
	})

	// The empty torrent is pending, so keep skipping full torrent.
	announceTickEvent{}.apply(state)
	announceTickEvent{}.apply(state)
	announceTickEvent{}.apply(state)

	// Remove a connection -- torrent is no longer full.
	c := state.conns.ActiveConns()[0]
	c.Close()
	// TODO(codyg): This is ugly. Conn fixtures aren't connected to our event
	// loop, so we have to manually trigger the event.
	connClosedEvent{c}.apply(state)

	mocks.eventLoop.expect(peerRemovedEvent{
		peerID:   c.PeerID(),
		infoHash: c.InfoHash(),
	})

	mocks.announceClient.EXPECT().
		Announce(
			full.dispatcher.Digest(),
			full.dispatcher.InfoHash(),
			false,
			announceclient.V2).
		Return(nil, time.Second, nil)

	announceTickEvent{}.apply(state)

	// Previously full torrent announced.
	mocks.eventLoop.expect(announceResultEvent{
		infoHash: full.dispatcher.InfoHash(),
	})
}
