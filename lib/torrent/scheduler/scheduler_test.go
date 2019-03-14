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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler/announcequeue"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"
	"github.com/uber/kraken/tracker/announceclient"
	"github.com/uber/kraken/utils/bitsetutil"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func TestDownloadTorrentWithSeederAndLeecher(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()

	seeder := mocks.newPeer(config)
	leecher := mocks.newPeer(config)

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).Times(2)

	seeder.writeTorrent(namespace, blob)
	require.NoError(seeder.scheduler.Download(namespace, blob.Digest))

	require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
	leecher.checkTorrent(t, namespace, blob)
}

func TestDownloadManyTorrentsWithSeederAndLeecher(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	namespace := core.TagFixture()

	seeder := mocks.newPeer(config)
	leecher := mocks.newPeer(config)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		blob := core.NewBlobFixture()

		mocks.metaInfoClient.EXPECT().Download(
			namespace, blob.Digest).Return(blob.MetaInfo, nil).Times(2)

		wg.Add(1)
		go func() {
			defer wg.Done()

			seeder.writeTorrent(namespace, blob)
			require.NoError(seeder.scheduler.Download(namespace, blob.Digest))

			require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
			leecher.checkTorrent(t, namespace, blob)
		}()
	}
	wg.Wait()
}

func TestDownloadManyTorrentsWithSeederAndManyLeechers(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	namespace := core.TagFixture()

	seeder := mocks.newPeer(config)
	leechers := mocks.newPeers(5, config)

	// Start seeding each torrent.
	blobs := make([]*core.BlobFixture, 5)
	for i := range blobs {
		blob := core.NewBlobFixture()
		blobs[i] = blob

		mocks.metaInfoClient.EXPECT().Download(
			namespace, blob.Digest).Return(blob.MetaInfo, nil).Times(6)

		seeder.writeTorrent(namespace, blob)
		require.NoError(seeder.scheduler.Download(namespace, blob.Digest))
	}

	var wg sync.WaitGroup
	for _, blob := range blobs {
		blob := blob
		for _, p := range leechers {
			p := p
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(p.scheduler.Download(namespace, blob.Digest))
				p.checkTorrent(t, namespace, blob)
			}()
		}
	}
	wg.Wait()
}

func TestDownloadTorrentWhenPeersAllHaveDifferentPiece(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	namespace := core.TagFixture()

	peers := mocks.newPeers(10, config)

	pieceLength := 256
	blob := core.SizedBlobFixture(uint64(len(peers)*pieceLength), uint64(pieceLength))

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).Times(len(peers))

	var wg sync.WaitGroup
	for i, p := range peers {
		tor, err := p.torrentArchive.CreateTorrent(namespace, blob.Digest)
		require.NoError(err)

		piece := make([]byte, pieceLength)
		start := i * pieceLength
		stop := (i + 1) * pieceLength
		copy(piece, blob.Content[start:stop])
		require.NoError(tor.WritePiece(piecereader.NewBuffer(piece), i))

		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(p.scheduler.Download(namespace, blob.Digest))
			p.checkTorrent(t, namespace, blob)
		}()
	}
	wg.Wait()
}

func TestSeederTTI(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).Times(2)

	clk := clock.NewMock()
	w := newEventWatcher()

	seeder := mocks.newPeer(config, withEventLoop(w), withClock(clk))
	seeder.writeTorrent(namespace, blob)
	require.NoError(seeder.scheduler.Download(namespace, blob.Digest))

	leecher := mocks.newPeer(config, withClock(clk))

	errc := make(chan error)
	go func() { errc <- leecher.scheduler.Download(namespace, blob.Digest) }()

	require.NoError(<-errc)
	leecher.checkTorrent(t, namespace, blob)

	// Conns expire...
	clk.Add(config.ConnTTI)

	clk.Add(config.PreemptionInterval)
	w.waitFor(t, preemptionTickEvent{})

	// Then seeding torrents expire.
	clk.Add(config.SeederTTI)

	waitForTorrentRemoved(t, seeder.scheduler, blob.MetaInfo.InfoHash())
	waitForTorrentRemoved(t, leecher.scheduler, blob.MetaInfo.InfoHash())

	require.False(hasConn(seeder.scheduler, leecher.pctx.PeerID, blob.MetaInfo.InfoHash()))
	require.False(hasConn(leecher.scheduler, seeder.pctx.PeerID, blob.MetaInfo.InfoHash()))

	// Idle seeder should keep around the torrent file so it can still serve content.
	_, err := seeder.torrentArchive.Stat(namespace, blob.Digest)
	require.NoError(err)
}

func TestLeecherTTI(t *testing.T) {
	t.Skip()

	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	clk := clock.NewMock()
	w := newEventWatcher()

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	mocks.metaInfoClient.EXPECT().Download(namespace, blob.Digest).Return(blob.MetaInfo, nil)

	p := mocks.newPeer(config, withEventLoop(w), withClock(clk))
	errc := make(chan error)
	go func() { errc <- p.scheduler.Download(namespace, blob.Digest) }()

	waitForTorrentAdded(t, p.scheduler, blob.MetaInfo.InfoHash())

	clk.Add(config.LeecherTTI)

	w.waitFor(t, preemptionTickEvent{})

	require.Equal(ErrTorrentTimeout, <-errc)

	// Idle leecher should delete torrent file to prevent it from being revived.
	_, err := p.torrentArchive.Stat(namespace, blob.Digest)
	require.True(os.IsNotExist(err))
}

func TestMultipleDownloadsForSameTorrentSucceed(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	// Allow any number of downloads due to concurrency below.
	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).AnyTimes()

	config := configFixture()

	seeder := mocks.newPeer(config)
	seeder.writeTorrent(namespace, blob)
	require.NoError(seeder.scheduler.Download(namespace, blob.Digest))

	leecher := mocks.newPeer(config)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Multiple goroutines should be able to wait on the same torrent.
			require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
		}()
	}
	wg.Wait()

	leecher.checkTorrent(t, namespace, blob)

	// After the torrent is complete, further calls to Download should succeed immediately.
	require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
}

func TestEmitStatsEventTriggers(t *testing.T) {
	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	clk := clock.NewMock()
	w := newEventWatcher()

	mocks.newPeer(config, withEventLoop(w), withClock(clk))

	clk.Add(config.EmitStatsInterval)
	w.waitFor(t, emitStatsEvent{})
}

func TestNetworkEvents(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	config.ConnTTI = 2 * time.Second
	config.ConnState.BlacklistDuration = 30 * time.Second

	seeder := mocks.newPeer(config)
	leecher := mocks.newPeer(config)

	// Torrent with 1 piece.
	blob := core.SizedBlobFixture(1, 1)
	namespace := core.TagFixture()

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).Times(2)

	seeder.writeTorrent(namespace, blob)
	require.NoError(seeder.scheduler.Download(namespace, blob.Digest))

	require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
	leecher.checkTorrent(t, namespace, blob)

	sid := seeder.pctx.PeerID
	lid := leecher.pctx.PeerID
	h := blob.MetaInfo.InfoHash()

	waitForConnRemoved(t, seeder.scheduler, lid, h)
	waitForConnRemoved(t, leecher.scheduler, sid, h)

	seederExpected := []*networkevent.Event{
		networkevent.AddTorrentEvent(h, sid, bitsetutil.FromBools(true), config.ConnState.MaxOpenConnectionsPerTorrent),
		networkevent.TorrentCompleteEvent(h, sid),
		networkevent.AddActiveConnEvent(h, sid, lid),
		networkevent.DropActiveConnEvent(h, sid, lid),
		networkevent.BlacklistConnEvent(h, sid, lid, config.ConnState.BlacklistDuration),
	}

	leecherExpected := []*networkevent.Event{
		networkevent.AddTorrentEvent(h, lid, bitsetutil.FromBools(false), config.ConnState.MaxOpenConnectionsPerTorrent),
		networkevent.AddActiveConnEvent(h, lid, sid),
		networkevent.RequestPieceEvent(h, lid, sid, 0),
		networkevent.ReceivePieceEvent(h, lid, sid, 0),
		networkevent.TorrentCompleteEvent(h, lid),
		networkevent.DropActiveConnEvent(h, lid, sid),
		networkevent.BlacklistConnEvent(h, lid, sid, config.ConnState.BlacklistDuration),
	}

	require.Equal(
		networkevent.StripTimestamps(seederExpected),
		networkevent.StripTimestamps(seeder.testProducer.Events()))

	require.Equal(
		networkevent.StripTimestamps(leecherExpected),
		networkevent.StripTimestamps(leecher.testProducer.Events()))
}

func TestPullInactiveTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).Times(2)

	seeder := mocks.newPeer(config)

	// Write torrent to disk, but don't add it the scheduler.
	seeder.writeTorrent(namespace, blob)

	// Force announce the scheduler for this torrent to simulate a peer which
	// is registered in tracker but does not have the torrent in memory.
	ac := announceclient.New(seeder.pctx, hashring.NoopPassiveRing(hostlist.Fixture(mocks.trackerAddr)), nil)
	ac.Announce(blob.Digest, blob.MetaInfo.InfoHash(), false, announceclient.V1)

	leecher := mocks.newPeer(config)

	require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
	leecher.checkTorrent(t, namespace, blob)
}

func TestSchedulerReload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	namespace := core.TagFixture()

	seeder := mocks.newPeer(config)
	leecher := mocks.newPeer(config)

	download := func() {
		blob := core.NewBlobFixture()

		mocks.metaInfoClient.EXPECT().Download(
			namespace, blob.Digest).Return(blob.MetaInfo, nil).Times(2)

		seeder.writeTorrent(namespace, blob)
		require.NoError(seeder.scheduler.Download(namespace, blob.Digest))

		require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
		leecher.checkTorrent(t, namespace, blob)
	}

	download()

	rs := makeReloadable(leecher.scheduler, func() announcequeue.Queue { return announcequeue.New() })
	config.ConnTTL += 5 * time.Minute
	rs.Reload(config)
	leecher.scheduler = rs.scheduler

	download()
}

func TestSchedulerRemoveTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	w := newEventWatcher()

	p := mocks.newPeer(configFixture(), withEventLoop(w))

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil)

	errc := make(chan error)
	go func() { errc <- p.scheduler.Download(namespace, blob.Digest) }()

	w.waitFor(t, newTorrentEvent{})

	require.NoError(p.scheduler.RemoveTorrent(blob.Digest))

	require.Equal(ErrTorrentRemoved, <-errc)

	_, err := p.torrentArchive.Stat(namespace, blob.Digest)
	require.True(os.IsNotExist(err))
}

func TestSchedulerProbe(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	p := mocks.newPeer(configFixture())

	require.NoError(p.scheduler.Probe())

	p.scheduler.Stop()

	require.Equal(ErrSchedulerStopped, p.scheduler.Probe())
}

type deadlockEvent struct {
	release chan struct{}
}

func (e deadlockEvent) apply(*state) {
	<-e.release
}

func TestSchedulerProbeTimeoutsIfDeadlocked(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	config.ProbeTimeout = 250 * time.Millisecond

	p := mocks.newPeer(config)

	require.NoError(p.scheduler.Probe())

	// Must release deadlock so Scheduler can shut down properly (only matters
	// for testing).
	release := make(chan struct{})
	p.scheduler.eventLoop.send(deadlockEvent{release})

	require.Equal(ErrSendEventTimedOut, p.scheduler.Probe())

	close(release)
}
