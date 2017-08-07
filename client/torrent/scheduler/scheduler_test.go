package scheduler

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"code.uber.internal/go-common.git/x/log"
	trackerservice "code.uber.internal/infra/kraken/tracker/service"
)

func init() {
	formatter := true
	logConfig := &log.Configuration{
		Level:         log.DebugLevel,
		Stdout:        true,
		TextFormatter: &formatter,
	}
	log.Configure(logConfig, true)
}

func TestDownloadTorrentWithSeederAndLeecher(t *testing.T) {
	require := require.New(t)

	stop := trackerservice.TestAnnouncer(trackerAddr)
	defer stop()

	config := genConfig()

	seederTM := genTorrentManager()
	defer seederTM.Delete()

	leecherTM := genTorrentManager()
	defer leecherTM.Delete()

	seeder, err := New(genPeerID(), "127.0.0.1", 6000, "sjc1", seederTM, config)
	require.NoError(err)
	defer seeder.Stop()

	leecher, err := New(genPeerID(), "127.0.0.1", 6001, "sjc1", leecherTM, config)
	require.NoError(err)
	defer leecher.Stop()

	mi, content := genTorrent(genTorrentOpts{})
	infoHash := mi.HashInfoBytes()
	seederTor := writeTorrent(seederTM, mi, content)
	leecherTor, err := leecherTM.CreateTorrent(infoHash, mi.InfoBytes)
	require.NoError(err)

	err = <-seeder.AddTorrent(seederTor, infoHash, mi.InfoBytes)
	require.NoError(err)

	err = <-leecher.AddTorrent(leecherTor, infoHash, mi.InfoBytes)
	require.NoError(err)

	checkContent(require, leecherTor, content)
}

func TestDownloadManyTorrentsWithSeederAndLeecher(t *testing.T) {
	require := require.New(t)

	stop := trackerservice.TestAnnouncer(trackerAddr)
	defer stop()

	config := genConfig()

	seederTM := genTorrentManager()
	defer seederTM.Delete()

	leecherTM := genTorrentManager()
	defer leecherTM.Delete()

	seeder, err := New(genPeerID(), "127.0.0.1", 6000, "sjc1", seederTM, config)
	require.NoError(err)
	defer seeder.Stop()

	leecher, err := New(genPeerID(), "127.0.0.1", 6001, "sjc1", leecherTM, config)
	require.NoError(err)
	defer leecher.Stop()

	var wg sync.WaitGroup
	for _, tor := range genTestTorrents(40, genTorrentOpts{}) {
		tor := tor
		wg.Add(1)
		go func() {
			defer wg.Done()

			infoHash := tor.Info.HashInfoBytes()
			seederTor := writeTorrent(seederTM, tor.Info, tor.Content)
			leecherTor, err := leecherTM.CreateTorrent(infoHash, tor.Info.InfoBytes)
			require.NoError(err)

			err = <-seeder.AddTorrent(seederTor, infoHash, tor.Info.InfoBytes)
			require.NoError(err)

			err = <-leecher.AddTorrent(leecherTor, infoHash, tor.Info.InfoBytes)
			require.NoError(err)

			checkContent(require, leecherTor, tor.Content)
		}()
	}
	wg.Wait()
}

func TestDownloadManyTorrentsWithSeederAndManyLeechers(t *testing.T) {
	require := require.New(t)

	stop := trackerservice.TestAnnouncer(trackerAddr)
	defer stop()

	config := genConfig()

	seederTM := genTorrentManager()
	defer seederTM.Delete()

	seeder, err := New(genPeerID(), "127.0.0.1", 6000, "sjc1", seederTM, config)
	require.NoError(err)
	defer seeder.Stop()

	leechers, stopAll := genTestPeers(5, 6001, config)
	defer stopAll()

	torrents := genTestTorrents(5, genTorrentOpts{})

	// Start seeding each torrent.
	for _, tor := range torrents {
		infoHash := tor.Info.HashInfoBytes()
		seederTor := writeTorrent(seederTM, tor.Info, tor.Content)

		err := <-seeder.AddTorrent(seederTor, infoHash, tor.Info.InfoBytes)
		require.NoError(err)
	}

	var wg sync.WaitGroup
	for _, tor := range torrents {
		tor := tor
		for _, p := range leechers {
			p := p
			wg.Add(1)
			go func() {
				defer wg.Done()

				infoHash := tor.Info.HashInfoBytes()

				leecherTor, err := p.TorrentManager.CreateTorrent(infoHash, tor.Info.InfoBytes)
				require.NoError(err)

				select {
				case err := <-p.Scheduler.AddTorrent(leecherTor, infoHash, tor.Info.InfoBytes):
					require.NoError(err)
				case <-time.After(10 * time.Second):
					t.Errorf("AddTorrent timeout scheduler=%s torrent=%s", p.Scheduler.peerID, infoHash)
					return
				}

				checkContent(require, leecherTor, tor.Content)
			}()
		}
	}
	wg.Wait()
}

func TestDownloadTorrentWhenPeersAllHaveDifferentPiece(t *testing.T) {
	require := require.New(t)

	stop := trackerservice.TestAnnouncer(trackerAddr)
	defer stop()

	config := genConfig()

	peers, stopAll := genTestPeers(10, 6000, config)
	defer stopAll()

	pieceLength := 256
	mi, content := genTorrent(genTorrentOpts{
		Size:        len(peers) * pieceLength,
		PieceLength: pieceLength,
	})
	infoHash := mi.HashInfoBytes()

	var wg sync.WaitGroup
	for i, p := range peers {
		partialContent := make([]byte, len(content))
		start := i * pieceLength
		stop := (i + 1) * pieceLength
		copy(partialContent[start:stop], content[start:stop])
		tor := writeTorrent(p.TorrentManager, mi, partialContent)

		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case err := <-p.Scheduler.AddTorrent(tor, infoHash, mi.InfoBytes):
				require.NoError(err)
			case <-time.After(10 * time.Second):
				t.Errorf("AddTorrent timeout scheduler=%s torrent=%s", p.Scheduler.peerID, infoHash)
				return
			}
			checkContent(require, tor, content)
		}()
	}
	wg.Wait()
}

func TestPeerAnnouncesPieceAfterDownloadingFromSeeder(t *testing.T) {
	require := require.New(t)

	stop := trackerservice.TestAnnouncer(trackerAddr)
	defer stop()

	mi, content := genTorrent(genTorrentOpts{})
	infoHash := mi.HashInfoBytes()

	// Each peer is allowed two connections, which allows them to establish both
	// a connection to the seeder and another peer.
	peerConfig := genConfig()
	peerConfig.MaxOpenConnectionsPerTorrent = 2

	peerA := genTestPeer(6000, peerConfig)
	defer peerA.Stop()
	peerATor, err := peerA.TorrentManager.CreateTorrent(infoHash, mi.InfoBytes)
	require.NoError(err)

	peerB := genTestPeer(6001, peerConfig)
	defer peerB.Stop()
	peerBTor, err := peerB.TorrentManager.CreateTorrent(infoHash, mi.InfoBytes)
	require.NoError(err)

	peerAErrc := peerA.Scheduler.AddTorrent(peerATor, infoHash, mi.InfoBytes)
	peerBErrc := peerB.Scheduler.AddTorrent(peerBTor, infoHash, mi.InfoBytes)

	// Wait for peerA and peerB to establish connections to one another before
	// starting the seeder, so their handshake bitfields are both guaranteed to
	// be empty.
	waitForConn(t, peerA.Scheduler, peerB.Scheduler.peerID, infoHash)
	waitForConn(t, peerB.Scheduler, peerA.Scheduler.peerID, infoHash)

	// The seeder is allowed only one connection, which means only one peer will
	// have access to the completed torrent, while the other is forced to rely
	// on the "trickle down" announce piece messages.
	seederConfig := genConfig()
	seederConfig.MaxOpenConnectionsPerTorrent = 1

	seeder := genTestPeer(6002, seederConfig)
	defer seeder.Stop()
	seederTor := writeTorrent(seeder.TorrentManager, mi, content)
	require.NoError(<-seeder.Scheduler.AddTorrent(seederTor, infoHash, mi.InfoBytes))

	require.NoError(<-peerAErrc)
	require.NoError(<-peerBErrc)

	checkContent(require, peerATor, content)
	checkContent(require, peerBTor, content)

	// Ensure that only one peer was able to open a connection to the seeder.
	require.NotEqual(
		hasConn(peerA.Scheduler, seeder.Scheduler.peerID, infoHash),
		hasConn(peerB.Scheduler, seeder.Scheduler.peerID, infoHash))
}
