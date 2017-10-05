package scheduler

import (
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/torlib"
	trackerservice "code.uber.internal/infra/kraken/tracker/service"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func TestDownloadTorrentWithSeederAndLeecher(t *testing.T) {
	require := require.New(t)

	trackerAddr, stop := trackerservice.TestAnnouncer()
	defer stop()

	config := configFixture(trackerAddr)

	seeder := testPeerFixture(config)
	defer seeder.Stop()

	leecher := testPeerFixture(config)
	defer leecher.Stop()

	tf := torlib.TestTorrentFileFixture()
	writeTorrent(seeder.TorrentArchive, tf.MetaInfo, tf.Content)
	leecherTor, err := leecher.TorrentArchive.CreateTorrent(tf.MetaInfo.InfoHash, tf.MetaInfo)
	require.NoError(err)

	err = <-seeder.Scheduler.AddTorrent(tf.MetaInfo)
	require.NoError(err)
	err = <-leecher.Scheduler.AddTorrent(tf.MetaInfo)
	require.NoError(err)
	checkContent(require, leecherTor, tf.Content)
}

func TestDownloadManyTorrentsWithSeederAndLeecher(t *testing.T) {
	require := require.New(t)

	trackerAddr, stop := trackerservice.TestAnnouncer()
	defer stop()

	config := configFixture(trackerAddr)

	seeder := testPeerFixture(config)
	defer seeder.Stop()

	leecher := testPeerFixture(config)
	defer leecher.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		tf := torlib.TestTorrentFileFixture()
		wg.Add(1)
		go func() {
			defer wg.Done()

			writeTorrent(seeder.TorrentArchive, tf.MetaInfo, tf.Content)
			leecherTor, err := leecher.TorrentArchive.CreateTorrent(tf.MetaInfo.InfoHash, tf.MetaInfo)
			require.NoError(err)

			err = <-seeder.Scheduler.AddTorrent(tf.MetaInfo)
			require.NoError(err)

			err = <-leecher.Scheduler.AddTorrent(tf.MetaInfo)
			require.NoError(err)

			checkContent(require, leecherTor, tf.Content)
		}()
	}
	wg.Wait()
}

func TestDownloadManyTorrentsWithSeederAndManyLeechers(t *testing.T) {
	require := require.New(t)

	trackerAddr, stop := trackerservice.TestAnnouncer()
	defer stop()

	config := configFixture(trackerAddr)

	seeder := testPeerFixture(config)
	defer seeder.Stop()

	leechers, stopAll := testPeerFixtures(5, config)
	defer stopAll()

	// Start seeding each torrent.
	torrentFiles := make([]*torlib.TestTorrentFile, 5)
	for i := range torrentFiles {
		tf := torlib.TestTorrentFileFixture()
		torrentFiles[i] = tf
		writeTorrent(seeder.TorrentArchive, tf.MetaInfo, tf.Content)
		err := <-seeder.Scheduler.AddTorrent(tf.MetaInfo)
		require.NoError(err)
	}

	var wg sync.WaitGroup
	for _, tf := range torrentFiles {
		tf := tf
		for _, p := range leechers {
			p := p
			wg.Add(1)
			go func() {
				defer wg.Done()
				leecherTor, err := p.TorrentArchive.CreateTorrent(tf.MetaInfo.InfoHash, tf.MetaInfo)
				require.NoError(err)

				select {
				case err := <-p.Scheduler.AddTorrent(tf.MetaInfo):
					require.NoError(err)
				case <-time.After(10 * time.Second):
					t.Errorf("AddTorrent timeout scheduler=%s torrent=%s", p.Scheduler.peerID, tf.MetaInfo.InfoHash)
					return
				}

				checkContent(require, leecherTor, tf.Content)
			}()
		}
	}
	wg.Wait()
}

func TestDownloadTorrentWhenPeersAllHaveDifferentPiece(t *testing.T) {
	require := require.New(t)

	trackerAddr, stop := trackerservice.TestAnnouncer()
	defer stop()

	config := configFixture(trackerAddr)

	peers, stopAll := testPeerFixtures(10, config)
	defer stopAll()

	pieceLength := 256
	tf := torlib.CustomTestTorrentFileFixture(len(peers)*pieceLength, pieceLength)

	var wg sync.WaitGroup
	for i, p := range peers {
		partialContent := make([]byte, len(tf.Content))
		start := i * pieceLength
		stop := (i + 1) * pieceLength
		copy(partialContent[start:stop], tf.Content[start:stop])
		tor := writeTorrent(p.TorrentArchive, tf.MetaInfo, partialContent)

		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case err := <-p.Scheduler.AddTorrent(tf.MetaInfo):
				require.NoError(err)
			case <-time.After(10 * time.Second):
				t.Errorf("AddTorrent timeout scheduler=%s torrent=%s", p.Scheduler.peerID, tf.MetaInfo.InfoHash)
				return
			}
			checkContent(require, tor, tf.Content)
		}()
	}
	wg.Wait()
}

func TestPeerAnnouncesPieceAfterDownloadingFromSeeder(t *testing.T) {
	require := require.New(t)

	trackerAddr, stop := trackerservice.TestAnnouncer()
	defer stop()

	tf := torlib.TestTorrentFileFixture()

	// Each peer is allowed two connections, which allows them to establish both
	// a connection to the seeder and another peer.
	peerConfig := configFixture(trackerAddr)
	peerConfig.ConnState.MaxOpenConnectionsPerTorrent = 2

	peerA := testPeerFixture(peerConfig)
	defer peerA.Stop()
	peerATor, err := peerA.TorrentArchive.CreateTorrent(tf.MetaInfo.InfoHash, tf.MetaInfo)
	require.NoError(err)

	peerB := testPeerFixture(peerConfig)
	defer peerB.Stop()
	peerBTor, err := peerB.TorrentArchive.CreateTorrent(tf.MetaInfo.InfoHash, tf.MetaInfo)
	require.NoError(err)

	peerAErrc := peerA.Scheduler.AddTorrent(tf.MetaInfo)
	peerBErrc := peerB.Scheduler.AddTorrent(tf.MetaInfo)

	// Wait for peerA and peerB to establish connections to one another before
	// starting the seeder, so their handshake bitfields are both guaranteed to
	// be empty.
	waitForConnEstablished(t, peerA.Scheduler, peerB.Scheduler.peerID, tf.MetaInfo.InfoHash)
	waitForConnEstablished(t, peerB.Scheduler, peerA.Scheduler.peerID, tf.MetaInfo.InfoHash)

	// The seeder is allowed only one connection, which means only one peer will
	// have access to the completed torrent, while the other is forced to rely
	// on the "trickle down" announce piece messages.
	seederConfig := configFixture(trackerAddr)
	seederConfig.ConnState.MaxOpenConnectionsPerTorrent = 1

	seeder := testPeerFixture(seederConfig)
	defer seeder.Stop()
	writeTorrent(seeder.TorrentArchive, tf.MetaInfo, tf.Content)
	require.NoError(<-seeder.Scheduler.AddTorrent(tf.MetaInfo))
	require.NoError(<-peerAErrc)
	require.NoError(<-peerBErrc)

	checkContent(require, peerATor, tf.Content)
	checkContent(require, peerBTor, tf.Content)

	// Ensure that only one peer was able to open a connection to the seeder.
	require.NotEqual(
		hasConn(peerA.Scheduler, seeder.Scheduler.peerID, tf.MetaInfo.InfoHash),
		hasConn(peerB.Scheduler, seeder.Scheduler.peerID, tf.MetaInfo.InfoHash))
}

func TestResourcesAreFreedAfterIdleTimeout(t *testing.T) {
	require := require.New(t)

	trackerAddr, stop := trackerservice.TestAnnouncer()
	defer stop()

	config := configFixture(trackerAddr)
	config.Conn.DisableThrottling = true

	tf := torlib.TestTorrentFileFixture()
	clk := clock.NewMock()
	w := newEventWatcher()

	seeder := testPeerFixture(config, withEventLoop(w), withClock(clk))
	defer seeder.Stop()
	writeTorrent(seeder.TorrentArchive, tf.MetaInfo, tf.Content)
	require.NoError(<-seeder.Scheduler.AddTorrent(tf.MetaInfo))

	leecher := testPeerFixture(config, withClock(clk))
	defer leecher.Stop()
	leecherTor, err := leecher.TorrentArchive.CreateTorrent(tf.MetaInfo.InfoHash, tf.MetaInfo)
	require.NoError(err)
	errc := leecher.Scheduler.AddTorrent(tf.MetaInfo)

	clk.Add(config.AnnounceInterval)

	require.NoError(<-errc)
	checkContent(require, leecherTor, tf.Content)

	// Conns expire...
	clk.Add(config.IdleConnTTL)

	clk.Add(config.PreemptionInterval)
	w.WaitFor(t, preemptionTickEvent{})

	// Then seeding torrents expire.
	clk.Add(config.IdleSeederTTL)

	waitForTorrentRemoved(t, seeder.Scheduler, tf.MetaInfo.InfoHash)
	waitForTorrentRemoved(t, leecher.Scheduler, tf.MetaInfo.InfoHash)

	require.False(hasConn(seeder.Scheduler, leecher.Scheduler.peerID, tf.MetaInfo.InfoHash))
	require.False(hasConn(leecher.Scheduler, seeder.Scheduler.peerID, tf.MetaInfo.InfoHash))
}

func TestMultipleAddTorrentsForSameTorrentSucceed(t *testing.T) {
	require := require.New(t)

	trackerAddr, stop := trackerservice.TestAnnouncer()
	defer stop()

	tf := torlib.TestTorrentFileFixture()
	config := configFixture(trackerAddr)

	seeder := testPeerFixture(config)
	defer seeder.Stop()
	writeTorrent(seeder.TorrentArchive, tf.MetaInfo, tf.Content)
	require.NoError(<-seeder.Scheduler.AddTorrent(tf.MetaInfo))

	leecher := testPeerFixture(config)
	defer leecher.Stop()
	leecherTor, err := leecher.TorrentArchive.CreateTorrent(tf.MetaInfo.InfoHash, tf.MetaInfo)
	require.NoError(err)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Multiple goroutines should be able to wait on the same torrent.
			require.NoError(<-leecher.Scheduler.AddTorrent(tf.MetaInfo))
		}()
	}
	wg.Wait()

	checkContent(require, leecherTor, tf.Content)

	// After the torrent is complete, further calls to AddTorrent should succeed immediately.
	require.NoError(<-leecher.Scheduler.AddTorrent(tf.MetaInfo))
}

func TestEmitStatsEventTriggers(t *testing.T) {
	trackerAddr, stop := trackerservice.TestAnnouncer()
	defer stop()

	config := configFixture(trackerAddr)
	clk := clock.NewMock()
	w := newEventWatcher()

	peer := testPeerFixture(config, withEventLoop(w), withClock(clk))
	defer peer.Stop()

	clk.Add(config.EmitStatsInterval)
	w.WaitFor(t, emitStatsEvent{})
}
