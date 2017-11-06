package scheduler

import (
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/announceclient"

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

	tf := torlib.TestTorrentFileFixture()

	mocks.metaInfoClient.EXPECT().Download(tf.MetaInfo.Name()).Return(tf.MetaInfo, nil).Times(2)

	seeder.writeTorrent(tf)
	require.NoError(<-seeder.scheduler.AddTorrent(tf.MetaInfo.Name()))

	require.NoError(<-leecher.scheduler.AddTorrent(tf.MetaInfo.Name()))
	leecher.checkTorrent(t, tf)
}

func TestDownloadManyTorrentsWithSeederAndLeecher(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()

	seeder := mocks.newPeer(config)
	leecher := mocks.newPeer(config)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		tf := torlib.TestTorrentFileFixture()

		mocks.metaInfoClient.EXPECT().Download(tf.MetaInfo.Name()).Return(tf.MetaInfo, nil).Times(2)

		wg.Add(1)
		go func() {
			defer wg.Done()

			seeder.writeTorrent(tf)
			require.NoError(<-seeder.scheduler.AddTorrent(tf.MetaInfo.Name()))

			require.NoError(<-leecher.scheduler.AddTorrent(tf.MetaInfo.Name()))
			leecher.checkTorrent(t, tf)
		}()
	}
	wg.Wait()
}

func TestDownloadManyTorrentsWithSeederAndManyLeechers(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()

	seeder := mocks.newPeer(config)
	leechers := mocks.newPeers(5, config)

	// Start seeding each torrent.
	torrentFiles := make([]*torlib.TestTorrentFile, 5)
	for i := range torrentFiles {
		tf := torlib.TestTorrentFileFixture()
		torrentFiles[i] = tf

		mocks.metaInfoClient.EXPECT().Download(tf.MetaInfo.Name()).Return(tf.MetaInfo, nil).Times(6)

		seeder.writeTorrent(tf)
		require.NoError(<-seeder.scheduler.AddTorrent(tf.MetaInfo.Name()))
	}

	var wg sync.WaitGroup
	for _, tf := range torrentFiles {
		tf := tf
		for _, p := range leechers {
			p := p
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case err := <-p.scheduler.AddTorrent(tf.MetaInfo.Name()):
					require.NoError(err)
					p.checkTorrent(t, tf)
				case <-time.After(10 * time.Second):
					t.Errorf("AddTorrent timeout scheduler=%s torrent=%s", p.pctx.PeerID, tf.MetaInfo.InfoHash)
				}
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

	peers := mocks.newPeers(10, config)

	pieceLength := 256
	tf := torlib.CustomTestTorrentFileFixture(len(peers)*pieceLength, pieceLength)

	mocks.metaInfoClient.EXPECT().Download(tf.MetaInfo.Name()).Return(tf.MetaInfo, nil).Times(len(peers))

	var wg sync.WaitGroup
	for i, p := range peers {
		tor, err := p.torrentArchive.GetTorrent(tf.MetaInfo.Name())
		require.NoError(err)

		piece := make([]byte, pieceLength)
		start := i * pieceLength
		stop := (i + 1) * pieceLength
		copy(piece, tf.Content[start:stop])
		require.NoError(tor.WritePiece(piece, i))

		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case err := <-p.scheduler.AddTorrent(tf.MetaInfo.Name()):
				require.NoError(err)
				p.checkTorrent(t, tf)
			case <-time.After(10 * time.Second):
				t.Errorf("AddTorrent timeout scheduler=%s torrent=%s", p.pctx.PeerID, tf.MetaInfo.InfoHash)
			}
		}()
	}
	wg.Wait()
}

func TestPeerAnnouncesPieceAfterDownloadingFromSeeder(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	tf := torlib.TestTorrentFileFixture()

	mocks.metaInfoClient.EXPECT().Download(tf.MetaInfo.Name()).Return(tf.MetaInfo, nil).Times(3)

	// Each peer is allowed two connections, which allows them to establish both
	// a connection to the seeder and another peer.
	peerConfig := configFixture()
	peerConfig.ConnState.MaxOpenConnectionsPerTorrent = 2

	peerA := mocks.newPeer(peerConfig)
	peerB := mocks.newPeer(peerConfig)

	peerAErrc := peerA.scheduler.AddTorrent(tf.MetaInfo.Name())
	peerBErrc := peerB.scheduler.AddTorrent(tf.MetaInfo.Name())

	// Wait for peerA and peerB to establish connections to one another before
	// starting the seeder, so their handshake bitfields are both guaranteed to
	// be empty.
	waitForConnEstablished(t, peerA.scheduler, peerB.pctx.PeerID, tf.MetaInfo.InfoHash)
	waitForConnEstablished(t, peerB.scheduler, peerA.pctx.PeerID, tf.MetaInfo.InfoHash)

	// The seeder is allowed only one connection, which means only one peer will
	// have access to the completed torrent, while the other is forced to rely
	// on the "trickle down" announce piece messages.
	seederConfig := configFixture()
	seederConfig.ConnState.MaxOpenConnectionsPerTorrent = 1

	seeder := mocks.newPeer(seederConfig)

	seeder.writeTorrent(tf)
	require.NoError(<-seeder.scheduler.AddTorrent(tf.MetaInfo.Name()))
	require.NoError(<-peerAErrc)
	require.NoError(<-peerBErrc)

	peerA.checkTorrent(t, tf)
	peerB.checkTorrent(t, tf)

	// Ensure that only one peer was able to open a connection to the seeder.
	require.NotEqual(
		hasConn(peerA.scheduler, seeder.pctx.PeerID, tf.MetaInfo.InfoHash),
		hasConn(peerB.scheduler, seeder.pctx.PeerID, tf.MetaInfo.InfoHash))
}

func TestResourcesAreFreedAfterIdleTimeout(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	config.Conn.DisableThrottling = true

	tf := torlib.TestTorrentFileFixture()

	mocks.metaInfoClient.EXPECT().Download(tf.MetaInfo.Name()).Return(tf.MetaInfo, nil).Times(2)

	clk := clock.NewMock()
	w := newEventWatcher()

	seeder := mocks.newPeer(config, withEventLoop(w), withClock(clk))
	seeder.writeTorrent(tf)
	require.NoError(<-seeder.scheduler.AddTorrent(tf.MetaInfo.Name()))

	leecher := mocks.newPeer(config, withClock(clk))
	errc := leecher.scheduler.AddTorrent(tf.MetaInfo.Name())

	clk.Add(config.AnnounceInterval)

	require.NoError(<-errc)
	leecher.checkTorrent(t, tf)

	// Conns expire...
	clk.Add(config.IdleConnTTL)

	clk.Add(config.PreemptionInterval)
	w.WaitFor(t, preemptionTickEvent{})

	// Then seeding torrents expire.
	clk.Add(config.IdleSeederTTL)

	waitForTorrentRemoved(t, seeder.scheduler, tf.MetaInfo.InfoHash)
	waitForTorrentRemoved(t, leecher.scheduler, tf.MetaInfo.InfoHash)

	require.False(hasConn(seeder.scheduler, leecher.pctx.PeerID, tf.MetaInfo.InfoHash))
	require.False(hasConn(leecher.scheduler, seeder.pctx.PeerID, tf.MetaInfo.InfoHash))
}

func TestMultipleAddTorrentsForSameTorrentSucceed(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	tf := torlib.TestTorrentFileFixture()

	// Allow any number of downloads due to concurrency below.
	mocks.metaInfoClient.EXPECT().Download(tf.MetaInfo.Name()).Return(tf.MetaInfo, nil).AnyTimes()

	config := configFixture()

	seeder := mocks.newPeer(config)
	seeder.writeTorrent(tf)
	require.NoError(<-seeder.scheduler.AddTorrent(tf.MetaInfo.Name()))

	leecher := mocks.newPeer(config)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Multiple goroutines should be able to wait on the same torrent.
			require.NoError(<-leecher.scheduler.AddTorrent(tf.MetaInfo.Name()))
		}()
	}
	wg.Wait()

	leecher.checkTorrent(t, tf)

	// After the torrent is complete, further calls to AddTorrent should succeed immediately.
	require.NoError(<-leecher.scheduler.AddTorrent(tf.MetaInfo.Name()))
}

func TestEmitStatsEventTriggers(t *testing.T) {
	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	clk := clock.NewMock()
	w := newEventWatcher()

	mocks.newPeer(config, withEventLoop(w), withClock(clk))

	clk.Add(config.EmitStatsInterval)
	w.WaitFor(t, emitStatsEvent{})
}

func stripTimestamps(events []networkevent.Event) []networkevent.Event {
	var res []networkevent.Event
	for _, e := range events {
		e.Time = time.Time{}
		res = append(res, e)
	}
	return res
}

func TestNetworkEvents(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	config.IdleConnTTL = 2 * time.Second

	seeder := mocks.newPeer(config)
	leecher := mocks.newPeer(config)

	// Torrent with 1 piece.
	tf := torlib.CustomTestTorrentFileFixture(1, 1)

	mocks.metaInfoClient.EXPECT().Download(tf.MetaInfo.Name()).Return(tf.MetaInfo, nil).Times(2)

	seeder.writeTorrent(tf)
	require.NoError(<-seeder.scheduler.AddTorrent(tf.MetaInfo.Name()))

	require.NoError(<-leecher.scheduler.AddTorrent(tf.MetaInfo.Name()))
	leecher.checkTorrent(t, tf)

	sid := seeder.pctx.PeerID
	lid := leecher.pctx.PeerID
	h := tf.MetaInfo.InfoHash

	waitForConnRemoved(t, seeder.scheduler, lid, h)
	waitForConnRemoved(t, leecher.scheduler, sid, h)

	seederExpected := []networkevent.Event{
		networkevent.AddTorrentEvent(h, sid, []bool{true}),
		networkevent.TorrentCompleteEvent(h, sid),
		networkevent.AddConnEvent(h, sid, lid),
		networkevent.DropConnEvent(h, sid, lid),
	}

	leecherExpected := []networkevent.Event{
		networkevent.AddTorrentEvent(h, lid, []bool{false}),
		networkevent.AddConnEvent(h, lid, sid),
		networkevent.ReceivePieceEvent(h, lid, sid, 0),
		networkevent.TorrentCompleteEvent(h, lid),
		networkevent.DropConnEvent(h, lid, sid),
	}

	require.Equal(
		stripTimestamps(seederExpected),
		stripTimestamps(seeder.testProducer.Events()))

	require.Equal(
		stripTimestamps(leecherExpected),
		stripTimestamps(leecher.testProducer.Events()))
}

func TestPullInactiveTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()

	tf := torlib.TestTorrentFileFixture()

	mocks.metaInfoClient.EXPECT().Download(tf.MetaInfo.Name()).Return(tf.MetaInfo, nil).Times(2)

	seeder := mocks.newPeer(config)

	// Write torrent to disk, but don't add it the scheduler.
	seeder.writeTorrent(tf)

	// Force announce the scheduler for this torrent to simulate a peer which
	// is registered in tracker but does not have the torrent in memory.
	ac := announceclient.Default(seeder.pctx, serverset.NewSingle(mocks.trackerAddr))
	ac.Announce(tf.MetaInfo.Info.Name, tf.MetaInfo.InfoHash, 0)

	leecher := mocks.newPeer(config)

	require.NoError(<-leecher.scheduler.AddTorrent(tf.MetaInfo.Name()))
	leecher.checkTorrent(t, tf)
}

func TestCancelTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()

	// Set high TTL to ensure conns are being closed due to cancel signal, not
	// natural expiration.
	config.IdleSeederTTL = time.Hour
	config.IdleConnTTL = time.Hour
	config.ConnTTL = time.Hour

	// Set high blacklist expiration to prevent second peer from immediately opening
	// a new connection to first peer after cancel.
	config.ConnState.InitialBlacklistExpiration = time.Minute

	p1 := mocks.newPeer(config)
	p2 := mocks.newPeer(config)

	tf := torlib.TestTorrentFileFixture()
	h := tf.MetaInfo.InfoHash

	mocks.metaInfoClient.EXPECT().Download(tf.MetaInfo.Name()).Return(tf.MetaInfo, nil).Times(2)

	// First peer will be cancelled.
	done := make(chan struct{})
	go func() {
		defer close(done)
		require.Error(ErrTorrentCancelled, <-p1.scheduler.AddTorrent(tf.MetaInfo.Name()))
	}()

	// (We don't really care what happens to the second peer).
	go p2.scheduler.AddTorrent(tf.MetaInfo.Name())

	waitForConnEstablished(t, p1.scheduler, p2.pctx.PeerID, h)
	waitForConnEstablished(t, p2.scheduler, p1.pctx.PeerID, h)

	p1.scheduler.CancelTorrent(tf.MetaInfo.Name())

	// Once first peer cancels, it should remove the connection to the second peer and
	// remove the torrent.
	<-done
	waitForConnRemoved(t, p1.scheduler, p2.pctx.PeerID, h)
	waitForTorrentRemoved(t, p1.scheduler, h)
}
