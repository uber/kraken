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
	"fmt"
	"reflect"
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

func (l *mockEventLoop) _next() (event, error) {
	select {
	case e := <-l.c:
		return e, nil
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("timed out waiting for event")
	}
}

// next returns the next event.
func (l *mockEventLoop) next() event {
	n, err := l._next()
	require.NoError(l.t, err)
	return n
}

// expect checks the next event is e and returns it.
func (l *mockEventLoop) expect(e event) event {
	n, err := l._next()
	require.NoError(l.t, err)
	require.Equal(l.t, n, e)
	return n
}

// expectType checks the next event is the same type as e and returns it.
func (l *mockEventLoop) expectType(e event) event {
	n, err := l._next()
	require.NoError(l.t, err)
	require.Equal(l.t, reflect.TypeOf(e).Name(), reflect.TypeOf(n).Name())
	return n
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
			announceclient.V1).
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

	// Spin up some fake peers we can handshake against.
	var peers []*core.PeerInfo
	for i := 0; i < 5; i++ {
		p, err := conn.NewFakePeer()
		require.NoError(err)
		defer p.Close()

		peers = append(peers, p.PeerInfo())
	}

	// Announce a torrent and fully saturate its connections.
	t1, err := state.addTorrent(_testNamespace, mocks.newTorrent(), true)
	require.NoError(err)

	mocks.announceClient.EXPECT().
		Announce(
			t1.dispatcher.Digest(),
			t1.dispatcher.InfoHash(),
			false,
			announceclient.V1).
		Return(peers, time.Second, nil)

	announceTickEvent{}.apply(state)

	mocks.eventLoop.expectType(announceResultEvent{}).apply(state)
	for range peers {
		mocks.eventLoop.expectType(outgoingConnEvent{}).apply(state)
	}

	// Add a second torrent (behind t1) and announce it. The first torrent is
	// full and should be skipped, instead directly announcing the second empty
	// torrent.
	t2, err := state.addTorrent(_testNamespace, mocks.newTorrent(), true)
	require.NoError(err)

	mocks.announceClient.EXPECT().
		Announce(
			t2.dispatcher.Digest(),
			t2.dispatcher.InfoHash(),
			false,
			announceclient.V1).
		Return(nil, time.Second, nil)

	announceTickEvent{}.apply(state)

	mocks.eventLoop.expect(announceResultEvent{
		infoHash: t2.dispatcher.InfoHash(),
	})

	// t1 is still full and t2 is pending, so nothing should happen.
	announceTickEvent{}.apply(state)
	announceTickEvent{}.apply(state)
	announceTickEvent{}.apply(state)

	// Close a random connection -- t1 is no longer full.
	c := state.conns.ActiveConns()[0]
	c.Close()

	// One of these is the conn closed, one of these is the peer removed. Can't
	// determine which order they happen in.
	// TODO(codyg): Fix this.
	mocks.eventLoop.next().apply(state)
	mocks.eventLoop.next().apply(state)

	mocks.announceClient.EXPECT().
		Announce(
			t1.dispatcher.Digest(),
			t1.dispatcher.InfoHash(),
			false,
			announceclient.V1).
		Return(nil, time.Second, nil)

	announceTickEvent{}.apply(state)

	// Previously full torrent (which now has open conn slot) announced.
	mocks.eventLoop.expect(announceResultEvent{
		infoHash: t1.dispatcher.InfoHash(),
	})
}
