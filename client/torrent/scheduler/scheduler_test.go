package scheduler

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/torlib"
	trackerservice "code.uber.internal/infra/kraken/tracker/service"
)

func init() {
	formatter := true
	logConfig := &log.Configuration{
		Level: log.DebugLevel,
		//Stdout:        true,
		TextFormatter: &formatter,
	}
	log.Configure(logConfig, true)
}

func TestDownloadTorrentWithSeederAndLeecher(t *testing.T) {
	require := require.New(t)

	trackerAddr, stop := trackerservice.TestAnnouncer()
	defer stop()

	config := genConfig(trackerAddr)

	seeder := genTestPeer(config)
	defer seeder.Stop()

	leecher := genTestPeer(config)
	defer leecher.Stop()

	tf := torlib.TestTorrentFileFixture()
	infoHash := tf.MetaInfo.GetInfoHash()
	infoBytes, err := tf.MetaInfo.Info.Serialize()
	require.NoError(err)
	seederTor := writeTorrent(seeder.TorrentManager, tf.MetaInfo, tf.Content)
	leecherTor, err := leecher.TorrentManager.CreateTorrent(infoHash, infoBytes)
	require.NoError(err)

	err = <-seeder.Scheduler.AddTorrent(seederTor, infoHash, infoBytes)
	require.NoError(err)

	err = <-leecher.Scheduler.AddTorrent(leecherTor, infoHash, infoBytes)
	require.NoError(err)

	checkContent(require, leecherTor, tf.Content)
}

func TestDownloadManyTorrentsWithSeederAndLeecher(t *testing.T) {
	require := require.New(t)

	trackerAddr, stop := trackerservice.TestAnnouncer()
	defer stop()

	config := genConfig(trackerAddr)

	seeder := genTestPeer(config)
	defer seeder.Stop()

	leecher := genTestPeer(config)
	defer leecher.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		tf := torlib.TestTorrentFileFixture()
		wg.Add(1)
		go func() {
			defer wg.Done()

			infoHash := tf.MetaInfo.GetInfoHash()
			infoBytes, err := tf.MetaInfo.Info.Serialize()
			require.NoError(err)
			seederTor := writeTorrent(seeder.TorrentManager, tf.MetaInfo, tf.Content)
			leecherTor, err := leecher.TorrentManager.CreateTorrent(infoHash, infoBytes)
			require.NoError(err)

			err = <-seeder.Scheduler.AddTorrent(seederTor, infoHash, infoBytes)
			require.NoError(err)

			err = <-leecher.Scheduler.AddTorrent(leecherTor, infoHash, infoBytes)
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

	config := genConfig(trackerAddr)

	seeder := genTestPeer(config)
	defer seeder.Stop()

	leechers, stopAll := genTestPeers(5, config)
	defer stopAll()

	// Start seeding each torrent.
	torrentFiles := make([]*torlib.TestTorrentFile, 5)
	for i := range torrentFiles {
		tf := torlib.TestTorrentFileFixture()
		torrentFiles[i] = tf

		infoHash := tf.MetaInfo.GetInfoHash()
		infoBytes, err := tf.MetaInfo.Info.Serialize()
		require.NoError(err)
		seederTor := writeTorrent(seeder.TorrentManager, tf.MetaInfo, tf.Content)

		err = <-seeder.Scheduler.AddTorrent(seederTor, infoHash, infoBytes)
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

				infoHash := tf.MetaInfo.GetInfoHash()
				infoBytes, err := tf.MetaInfo.Info.Serialize()
				require.NoError(err)
				leecherTor, err := p.TorrentManager.CreateTorrent(infoHash, infoBytes)
				require.NoError(err)

				select {
				case err := <-p.Scheduler.AddTorrent(leecherTor, infoHash, infoBytes):
					require.NoError(err)
				case <-time.After(10 * time.Second):
					t.Errorf("AddTorrent timeout scheduler=%s torrent=%s", p.Scheduler.peerID, infoHash)
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

	config := genConfig(trackerAddr)

	peers, stopAll := genTestPeers(10, config)
	defer stopAll()

	pieceLength := 256
	tf := torlib.CustomTestTorrentFileFixture(len(peers)*pieceLength, pieceLength)
	infoHash := tf.MetaInfo.GetInfoHash()
	infoBytes, err := tf.MetaInfo.Info.Serialize()
	require.NoError(err)

	var wg sync.WaitGroup
	for i, p := range peers {
		partialContent := make([]byte, len(tf.Content))
		start := i * pieceLength
		stop := (i + 1) * pieceLength
		copy(partialContent[start:stop], tf.Content[start:stop])
		tor := writeTorrent(p.TorrentManager, tf.MetaInfo, partialContent)

		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case err := <-p.Scheduler.AddTorrent(tor, infoHash, infoBytes):
				require.NoError(err)
			case <-time.After(10 * time.Second):
				t.Errorf("AddTorrent timeout scheduler=%s torrent=%s", p.Scheduler.peerID, infoHash)
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
	infoHash := tf.MetaInfo.GetInfoHash()
	infoBytes, err := tf.MetaInfo.Info.Serialize()
	require.NoError(err)

	// Each peer is allowed two connections, which allows them to establish both
	// a connection to the seeder and another peer.
	peerConfig := genConfig(trackerAddr)
	peerConfig.MaxOpenConnectionsPerTorrent = 2

	peerA := genTestPeer(peerConfig)
	defer peerA.Stop()
	peerATor, err := peerA.TorrentManager.CreateTorrent(infoHash, infoBytes)
	require.NoError(err)

	peerB := genTestPeer(peerConfig)
	defer peerB.Stop()
	peerBTor, err := peerB.TorrentManager.CreateTorrent(infoHash, infoBytes)
	require.NoError(err)

	peerAErrc := peerA.Scheduler.AddTorrent(peerATor, infoHash, infoBytes)
	peerBErrc := peerB.Scheduler.AddTorrent(peerBTor, infoHash, infoBytes)

	// Wait for peerA and peerB to establish connections to one another before
	// starting the seeder, so their handshake bitfields are both guaranteed to
	// be empty.
	waitForConnEstablished(t, peerA.Scheduler, peerB.Scheduler.peerID, infoHash)
	waitForConnEstablished(t, peerB.Scheduler, peerA.Scheduler.peerID, infoHash)

	// The seeder is allowed only one connection, which means only one peer will
	// have access to the completed torrent, while the other is forced to rely
	// on the "trickle down" announce piece messages.
	seederConfig := genConfig(trackerAddr)
	seederConfig.MaxOpenConnectionsPerTorrent = 1

	seeder := genTestPeer(seederConfig)
	defer seeder.Stop()
	seederTor := writeTorrent(seeder.TorrentManager, tf.MetaInfo, tf.Content)
	require.NoError(<-seeder.Scheduler.AddTorrent(seederTor, infoHash, infoBytes))

	require.NoError(<-peerAErrc)
	require.NoError(<-peerBErrc)

	checkContent(require, peerATor, tf.Content)
	checkContent(require, peerBTor, tf.Content)

	// Ensure that only one peer was able to open a connection to the seeder.
	require.NotEqual(
		hasConn(peerA.Scheduler, seeder.Scheduler.peerID, infoHash),
		hasConn(peerB.Scheduler, seeder.Scheduler.peerID, infoHash))
}

func TestResourcesAreFreedAfterIdleTimeout(t *testing.T) {
	require := require.New(t)

	trackerAddr, stop := trackerservice.TestAnnouncer()
	defer stop()

	tf := torlib.TestTorrentFileFixture()
	infoHash := tf.MetaInfo.GetInfoHash()
	infoBytes, err := tf.MetaInfo.Info.Serialize()
	require.NoError(err)

	config := genConfig(trackerAddr)
	config.IdleSeederTTL = 1 * time.Second

	seeder := genTestPeer(config)
	defer seeder.Stop()
	seederTor := writeTorrent(seeder.TorrentManager, tf.MetaInfo, tf.Content)
	require.NoError(<-seeder.Scheduler.AddTorrent(seederTor, infoHash, infoBytes))

	leecher := genTestPeer(config)
	defer leecher.Stop()
	leecherTor, err := leecher.TorrentManager.CreateTorrent(infoHash, infoBytes)
	require.NoError(err)
	require.NoError(<-leecher.Scheduler.AddTorrent(leecherTor, infoHash, infoBytes))

	checkContent(require, leecherTor, tf.Content)

	waitForDispatcherRemoved(t, seeder.Scheduler, infoHash)
	waitForDispatcherRemoved(t, leecher.Scheduler, infoHash)

	require.False(hasConn(seeder.Scheduler, leecher.Scheduler.peerID, infoHash))
	require.False(hasConn(leecher.Scheduler, seeder.Scheduler.peerID, infoHash))
}
