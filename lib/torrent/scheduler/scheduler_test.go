package scheduler

import (
	"os"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/utils/memsize"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestDownloadTorrentWithSeederAndLeecher(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()

	seeder := mocks.newPeer(config)
	leecher := mocks.newPeer(config)

	blob := core.NewBlobFixture()

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.MetaInfo.Name()).Return(blob.MetaInfo, nil).Times(2)

	seeder.writeTorrent(blob)
	require.NoError(<-seeder.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))

	require.NoError(<-leecher.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))
	leecher.checkTorrent(t, blob)
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
		blob := core.NewBlobFixture()

		mocks.metaInfoClient.EXPECT().Download(
			namespace, blob.MetaInfo.Name()).Return(blob.MetaInfo, nil).Times(2)

		wg.Add(1)
		go func() {
			defer wg.Done()

			seeder.writeTorrent(blob)
			require.NoError(<-seeder.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))

			require.NoError(<-leecher.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))
			leecher.checkTorrent(t, blob)
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
	blobs := make([]*core.BlobFixture, 5)
	for i := range blobs {
		blob := core.NewBlobFixture()
		blobs[i] = blob

		mocks.metaInfoClient.EXPECT().Download(
			namespace, blob.MetaInfo.Name()).Return(blob.MetaInfo, nil).Times(6)

		seeder.writeTorrent(blob)
		require.NoError(<-seeder.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))
	}

	var wg sync.WaitGroup
	for _, blob := range blobs {
		blob := blob
		for _, p := range leechers {
			p := p
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case err := <-p.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()):
					require.NoError(err)
					p.checkTorrent(t, blob)
				case <-time.After(10 * time.Second):
					t.Errorf("AddTorrent timeout scheduler=%s torrent=%s", p.pctx.PeerID, blob.MetaInfo.InfoHash)
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
	blob := core.SizedBlobFixture(uint64(len(peers)*pieceLength), uint64(pieceLength))

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.MetaInfo.Name()).Return(blob.MetaInfo, nil).Times(len(peers))

	var wg sync.WaitGroup
	for i, p := range peers {
		tor, err := p.torrentArchive.CreateTorrent(namespace, blob.MetaInfo.Name())
		require.NoError(err)

		piece := make([]byte, pieceLength)
		start := i * pieceLength
		stop := (i + 1) * pieceLength
		copy(piece, blob.Content[start:stop])
		require.NoError(tor.WritePiece(storage.NewPieceReaderBuffer(piece), i))

		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case err := <-p.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()):
				require.NoError(err)
				p.checkTorrent(t, blob)
			case <-time.After(10 * time.Second):
				t.Errorf("AddTorrent timeout scheduler=%s torrent=%s", p.pctx.PeerID, blob.MetaInfo.InfoHash)
			}
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

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.MetaInfo.Name()).Return(blob.MetaInfo, nil).Times(2)

	clk := clock.NewMock()
	w := newEventWatcher()

	seeder := mocks.newPeer(config, withEventLoop(w), withClock(clk))
	seeder.writeTorrent(blob)
	require.NoError(<-seeder.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))

	clk.Add(config.AnnounceInterval)

	leecher := mocks.newPeer(config, withClock(clk))
	errc := leecher.scheduler.AddTorrent(namespace, blob.MetaInfo.Name())

	clk.Add(config.AnnounceInterval)

	require.NoError(<-errc)
	leecher.checkTorrent(t, blob)

	// Conns expire...
	clk.Add(config.ConnTTI)

	clk.Add(config.PreemptionInterval)
	w.WaitFor(t, preemptionTickEvent{})

	// Then seeding torrents expire.
	clk.Add(config.SeederTTI)

	waitForTorrentRemoved(t, seeder.scheduler, blob.MetaInfo.InfoHash)
	waitForTorrentRemoved(t, leecher.scheduler, blob.MetaInfo.InfoHash)

	require.False(hasConn(seeder.scheduler, leecher.pctx.PeerID, blob.MetaInfo.InfoHash))
	require.False(hasConn(leecher.scheduler, seeder.pctx.PeerID, blob.MetaInfo.InfoHash))

	// Idle seeder should keep around the torrent file so it can still serve content.
	_, err := seeder.torrentArchive.Stat(blob.MetaInfo.Name())
	require.NoError(err)
}

func TestLeecherTTI(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	clk := clock.NewMock()
	w := newEventWatcher()

	blob := core.NewBlobFixture()

	mocks.metaInfoClient.EXPECT().Download(namespace, blob.MetaInfo.Name()).Return(blob.MetaInfo, nil)

	p := mocks.newPeer(config, withEventLoop(w), withClock(clk))
	errc := p.scheduler.AddTorrent(namespace, blob.MetaInfo.Name())

	waitForTorrentAdded(t, p.scheduler, blob.MetaInfo.InfoHash)

	clk.Add(config.LeecherTTI)

	w.WaitFor(t, preemptionTickEvent{})

	require.Equal(ErrTorrentTimeout, <-errc)

	// Idle leecher should delete torrent file to prevent it from being revived.
	_, err := p.torrentArchive.Stat(blob.MetaInfo.Name())
	require.True(os.IsNotExist(err))
}

func TestMultipleAddTorrentsForSameTorrentSucceed(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	blob := core.NewBlobFixture()

	// Allow any number of downloads due to concurrency below.
	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.MetaInfo.Name()).Return(blob.MetaInfo, nil).AnyTimes()

	config := configFixture()

	seeder := mocks.newPeer(config)
	seeder.writeTorrent(blob)
	require.NoError(<-seeder.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))

	leecher := mocks.newPeer(config)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Multiple goroutines should be able to wait on the same torrent.
			require.NoError(<-leecher.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))
		}()
	}
	wg.Wait()

	leecher.checkTorrent(t, blob)

	// After the torrent is complete, further calls to AddTorrent should succeed immediately.
	require.NoError(<-leecher.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))
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

func TestNetworkEvents(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()
	config.ConnTTI = 2 * time.Second

	seeder := mocks.newPeer(config)
	leecher := mocks.newPeer(config)

	// Torrent with 1 piece.
	blob := core.SizedBlobFixture(1, 1)

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.MetaInfo.Name()).Return(blob.MetaInfo, nil).Times(2)

	seeder.writeTorrent(blob)
	require.NoError(<-seeder.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))

	require.NoError(<-leecher.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))
	leecher.checkTorrent(t, blob)

	sid := seeder.pctx.PeerID
	lid := leecher.pctx.PeerID
	h := blob.MetaInfo.InfoHash

	waitForConnRemoved(t, seeder.scheduler, lid, h)
	waitForConnRemoved(t, leecher.scheduler, sid, h)

	seederExpected := []*networkevent.Event{
		networkevent.AddTorrentEvent(h, sid, storage.BitSetFixture(true), config.ConnState.MaxOpenConnectionsPerTorrent),
		networkevent.TorrentCompleteEvent(h, sid),
		networkevent.AddActiveConnEvent(h, sid, lid),
		networkevent.DropActiveConnEvent(h, sid, lid),
		networkevent.BlacklistConnEvent(h, sid, lid, config.ConnState.BlacklistDuration),
	}

	leecherExpected := []*networkevent.Event{
		networkevent.AddTorrentEvent(h, lid, storage.BitSetFixture(false), config.ConnState.MaxOpenConnectionsPerTorrent),
		networkevent.AddActiveConnEvent(h, lid, sid),
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

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.MetaInfo.Name()).Return(blob.MetaInfo, nil).Times(2)

	seeder := mocks.newPeer(config)

	// Write torrent to disk, but don't add it the scheduler.
	seeder.writeTorrent(blob)

	// Force announce the scheduler for this torrent to simulate a peer which
	// is registered in tracker but does not have the torrent in memory.
	ac := announceclient.New(seeder.pctx, serverset.NewSingle(mocks.trackerAddr))
	ac.Announce(blob.MetaInfo.Info.Name, blob.MetaInfo.InfoHash, false)

	leecher := mocks.newPeer(config)

	require.NoError(<-leecher.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))
	leecher.checkTorrent(t, blob)
}

func TestSchedulerReload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	config := configFixture()

	seeder := mocks.newPeer(config)
	leecher := mocks.newPeer(config)

	download := func() {
		blob := core.NewBlobFixture()

		mocks.metaInfoClient.EXPECT().Download(
			namespace, blob.MetaInfo.Name()).Return(blob.MetaInfo, nil).Times(2)

		seeder.writeTorrent(blob)
		require.NoError(<-seeder.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))

		require.NoError(<-leecher.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))
		leecher.checkTorrent(t, blob)
	}

	download()

	config.ConnTTL = 45 * time.Minute
	s, err := Reload(leecher.scheduler, config, tally.NewTestScope("", nil))
	require.NoError(err)
	leecher.scheduler = s

	download()
}

func TestSchedulerRemoveTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTestMocks(t)
	defer cleanup()

	p := mocks.newPeer(configFixture())

	blob := core.NewBlobFixture()

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.MetaInfo.Name()).Return(blob.MetaInfo, nil)

	errc := p.scheduler.AddTorrent(namespace, blob.MetaInfo.Name())

	done := make(chan struct{})
	go func() {
		defer close(done)
		require.Equal(ErrTorrentRemoved, <-errc)
	}()

	require.NoError(<-p.scheduler.RemoveTorrent(blob.MetaInfo.Name()))

	<-done

	_, err := p.torrentArchive.Stat(blob.MetaInfo.Name())
	require.True(os.IsNotExist(err))
}

// BENCHMARKS

// NOTE: You'll need to increase your fd limit to around 4096 to run this benchmark.
// You can do this with `ulimit -n 4096`.
func BenchmarkPieceUploadingAndDownloading(b *testing.B) {
	require := require.New(b)

	config := configFixture()
	config.AnnounceInterval = 50 * time.Millisecond

	b.ResetTimer()

	for i := 0; i < b.N; i++ {

		b.StopTimer()

		mocks, cleanup := newTestMocks(b)
		defer func() {
			if err := recover(); err != nil {
				cleanup()
				panic(err)
			}
		}()

		seeder := mocks.newPeer(config)

		var blobs []*core.BlobFixture
		for i := 0; i < 10; i++ {
			blob := core.SizedBlobFixture(50*memsize.MB, 128*memsize.KB)
			blobs = append(blobs, blob)

			mocks.metaInfoClient.EXPECT().Download(
				namespace, blob.MetaInfo.Name()).Return(blob.MetaInfo, nil).AnyTimes()

			seeder.writeTorrent(blob)
			require.NoError(<-seeder.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))
		}

		peers := mocks.newPeers(10, config)

		b.StartTimer()
		var wg sync.WaitGroup
		for _, p := range peers {
			for _, blob := range blobs {
				wg.Add(1)
				go func(p *testPeer, blob *core.BlobFixture) {
					defer wg.Done()
					require.NoError(<-p.scheduler.AddTorrent(namespace, blob.MetaInfo.Name()))
				}(p, blob)
			}
		}
		wg.Wait()
		b.StopTimer()

		cleanup()
	}
}
