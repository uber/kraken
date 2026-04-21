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
	"io"
	"sync"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/torrent/scheduler/conn"
	"github.com/uber/kraken/lib/torrent/scheduler/connstate"
	"github.com/uber/kraken/lib/torrent/scheduler/dispatch"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"
	"github.com/uber/kraken/utils/log"

	"github.com/stretchr/testify/require"
)

// checkTorrentB is a benchmark-compatible version of checkTorrent.
func checkTorrentB(b *testing.B, p *testPeer, namespace string, blob *core.BlobFixture) {
	b.Helper()
	require := require.New(b)

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

// benchConfig returns a scheduler config tuned for benchmarking.
func benchConfig() Config {
	return Config{
		ConnState: connstate.Config{
			MaxOpenConnectionsPerTorrent: 20,
		},
		Dispatch:   dispatch.Config{},
		Conn:       conn.ConfigFixture(),
		TorrentLog: log.Config{Disable: true},
	}.applyDefaults()
}

// BenchmarkE2E_SingleSeederSingleLeecher benchmarks downloading a blob
// from one seeder to one leecher through the full P2P pipeline:
// scheduler -> tracker announce -> handshake -> piece dispatch -> storage.
func BenchmarkE2E_SingleSeederSingleLeecher(b *testing.B) {
	require := require.New(b)

	mocks, cleanup := newTestMocks(b)
	defer cleanup()

	config := benchConfig()
	namespace := core.TagFixture()

	seeder := mocks.newPeer(config)

	// 64KB blob, 4KB pieces = 16 pieces. Realistic small Docker layer.
	blob := core.SizedBlobFixture(64*1024, 4*1024)

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).AnyTimes()

	seeder.writeTorrent(namespace, blob)
	require.NoError(seeder.scheduler.Download(namespace, blob.Digest))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		leecher := mocks.newPeer(config)
		require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
		checkTorrentB(b, leecher, namespace, blob)
		leecher.scheduler.Stop()
	}

	b.StopTimer()
}

// BenchmarkE2E_SingleSeederMultiLeecher benchmarks downloading a blob
// from one seeder to multiple concurrent leechers. Exercises peer
// coordination, connection management, and piece distribution.
func BenchmarkE2E_SingleSeederMultiLeecher(b *testing.B) {
	require := require.New(b)

	mocks, cleanup := newTestMocks(b)
	defer cleanup()

	config := benchConfig()
	namespace := core.TagFixture()

	seeder := mocks.newPeer(config)

	// 256KB blob, 4KB pieces = 64 pieces. Exercises piece selection.
	blob := core.SizedBlobFixture(256*1024, 4*1024)

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).AnyTimes()

	seeder.writeTorrent(namespace, blob)
	require.NoError(seeder.scheduler.Download(namespace, blob.Digest))

	numLeechers := 5

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		leechers := make([]*testPeer, numLeechers)
		for j := 0; j < numLeechers; j++ {
			leechers[j] = mocks.newPeer(config)
		}

		var wg sync.WaitGroup
		for _, l := range leechers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(l.scheduler.Download(namespace, blob.Digest))
				checkTorrentB(b, l, namespace, blob)
			}()
		}
		wg.Wait()

		for _, l := range leechers {
			l.scheduler.Stop()
		}
	}

	b.StopTimer()
}

// BenchmarkE2E_PeerSwarm benchmarks a swarm where all peers start
// with different pieces and must cooperate to complete. This is the
// core P2P distribution scenario.
func BenchmarkE2E_PeerSwarm(b *testing.B) {
	require := require.New(b)

	mocks, cleanup := newTestMocks(b)
	defer cleanup()

	config := benchConfig()
	namespace := core.TagFixture()

	numPeers := 5
	pieceLength := 256
	blobSize := uint64(numPeers * pieceLength)

	blob := core.SizedBlobFixture(blobSize, uint64(pieceLength))

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).AnyTimes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		peers := mocks.newPeers(numPeers, config)

		// Give each peer exactly one unique piece.
		for j, p := range peers {
			tor, err := p.torrentArchive.CreateTorrent(namespace, blob.Digest)
			require.NoError(err)

			piece := make([]byte, pieceLength)
			start := j * pieceLength
			stop := (j + 1) * pieceLength
			copy(piece, blob.Content[start:stop])
			require.NoError(tor.WritePiece(piecereader.NewBuffer(piece), j))
		}

		var wg sync.WaitGroup
		for _, p := range peers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(p.scheduler.Download(namespace, blob.Digest))
				checkTorrentB(b, p, namespace, blob)
			}()
		}
		wg.Wait()

		for _, p := range peers {
			p.scheduler.Stop()
		}
	}

	b.StopTimer()
}

// BenchmarkE2E_LargeBlob benchmarks downloading a larger blob (1MB)
// to exercise the storage pipeline with more pieces and larger I/O.
func BenchmarkE2E_LargeBlob(b *testing.B) {
	require := require.New(b)

	mocks, cleanup := newTestMocks(b)
	defer cleanup()

	config := benchConfig()
	namespace := core.TagFixture()

	seeder := mocks.newPeer(config)

	// 1MB blob, 64KB pieces = 16 pieces.
	blob := core.SizedBlobFixture(1024*1024, 64*1024)

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).AnyTimes()

	seeder.writeTorrent(namespace, blob)
	require.NoError(seeder.scheduler.Download(namespace, blob.Digest))

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(1024 * 1024)

	for i := 0; i < b.N; i++ {
		leecher := mocks.newPeer(config)
		require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
		checkTorrentB(b, leecher, namespace, blob)
		leecher.scheduler.Stop()
	}

	b.StopTimer()
}

// BenchmarkE2E_ManyTorrents benchmarks downloading many small torrents
// concurrently from the same seeder. Exercises the scheduler event
// loop, torrent control management, and announce pipeline.
func BenchmarkE2E_ManyTorrents(b *testing.B) {
	require := require.New(b)

	mocks, cleanup := newTestMocks(b)
	defer cleanup()

	config := benchConfig()
	namespace := core.TagFixture()

	seeder := mocks.newPeer(config)

	numTorrents := 10
	blobs := make([]*core.BlobFixture, numTorrents)
	for i := range blobs {
		blob := core.SizedBlobFixture(16*1024, 4*1024)
		blobs[i] = blob

		mocks.metaInfoClient.EXPECT().Download(
			namespace, blob.Digest).Return(blob.MetaInfo, nil).AnyTimes()

		seeder.writeTorrent(namespace, blob)
		require.NoError(seeder.scheduler.Download(namespace, blob.Digest))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		leecher := mocks.newPeer(config)

		var wg sync.WaitGroup
		for _, blob := range blobs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
				checkTorrentB(b, leecher, namespace, blob)
			}()
		}
		wg.Wait()
		leecher.scheduler.Stop()
	}

	b.StopTimer()
}
