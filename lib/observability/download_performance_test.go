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
package observability

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/utils/memsize"
)

func TestGetSizeTag(t *testing.T) {
	tests := map[string]struct {
		size     uint64
		expected string
	}{
		"below min":       {memsize.MB, _xsmall},
		"above max":       {30 * memsize.GB, _xxlarge},
		"between buckets": {3 * memsize.GB, _large},
		"exact bucket":    {1 * memsize.GB, _large},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tt.expected, getSizeTag(tt.size))
		})
	}
}

func TestBucketsConfiguredCorrectly(t *testing.T) {
	for i := 0; i < len(_downloadLatencyBuckets)-1; i++ {
		require.True(t, _downloadLatencyBuckets[i] < _downloadLatencyBuckets[i+1])
	}

	for i := 0; i < len(_downloadThroughputBuckets)-1; i++ {
		require.True(t, _downloadThroughputBuckets[i] < _downloadThroughputBuckets[i+1])
	}
}

func TestEmitDownloadPerformance(t *testing.T) {
	t.Run("latency", func(t *testing.T) {
		require := require.New(t)
		stats := tally.NewTestScope("", nil)

		EmitDownloadPerformance(stats, TORRENT_DOWNLOAD, 3*int64(memsize.MB), 220*time.Millisecond)
		EmitDownloadPerformance(stats, TORRENT_DOWNLOAD, 10*int64(memsize.GB), 20*time.Minute)

		snapshot := stats.Snapshot()
		histograms := snapshot.Histograms()
		downloadTimeXXsmallKey := "download_time+size=0B-5MiB,version=4"
		downloadTimeXXsmall, ok := histograms[downloadTimeXXsmallKey]
		require.True(ok)
		require.Equal(int64(1), downloadTimeXXsmall.Durations()[500*time.Millisecond])

		downloadTimeXXlargeKey := "download_time+size=10GiBplus,version=4"
		downloadTimeXXlarge, ok := histograms[downloadTimeXXlargeKey]
		require.True(ok)
		require.Equal(int64(1), downloadTimeXXlarge.Durations()[1260*time.Second])
	})

	t.Run("throughput", func(t *testing.T) {
		require := require.New(t)
		stats := tally.NewTestScope("", nil)

		EmitDownloadPerformance(stats, TORRENT_DOWNLOAD, 3*int64(memsize.MB), 2*time.Second)   // 1.5 MiB/s
		EmitDownloadPerformance(stats, TORRENT_DOWNLOAD, 10*int64(memsize.GB), 20*time.Minute) // 8.53 MiB/s

		snapshot := stats.Snapshot()
		histograms := snapshot.Histograms()

		downloadThroughputXXsmallKey := "download_throughput+size=0B-5MiB,version=2"
		downloadThroughputXXsmall, ok := histograms[downloadThroughputXXsmallKey]
		require.True(ok)
		require.Equal(int64(1), downloadThroughputXXsmall.Values()[1.5])

		downloadThroughputXXlargeKey := "download_throughput+size=10GiBplus,version=2"
		downloadThroughputXXlarge, ok := histograms[downloadThroughputXXlargeKey]
		require.True(ok)
		require.Equal(int64(1), downloadThroughputXXlarge.Values()[9])
	})

	t.Run("extremely small or large throughput", func(t *testing.T) {
		require := require.New(t)
		stats := tally.NewTestScope("", nil)

		EmitDownloadPerformance(stats, METAINFO_DOWNLOAD, 3*int64(memsize.MB), 2*time.Millisecond) // 1500 MiB/s
		EmitDownloadPerformance(stats, METAINFO_DOWNLOAD, 3*int64(memsize.MB), 1*time.Hour)        // 0.000833333333 MiB/s

		snapshot := stats.Snapshot()
		histograms := snapshot.Histograms()

		downloadThroughputXsmallKey := "metainfo_download_throughput+torrent_size=0B-5MiB,version=2"
		downloadThroughputXsmall, ok := histograms[downloadThroughputXsmallKey]
		require.True(ok)
		require.Equal(int64(1), downloadThroughputXsmall.Values()[math.MaxFloat64])
		require.Equal(int64(1), downloadThroughputXsmall.Values()[0.1])
	})

	t.Run("p2p torrent leeching throughput", func(t *testing.T) {
		require := require.New(t)
		stats := tally.NewTestScope("", nil)

		EmitDownloadPerformance(stats, TORRENT_LEECH, 3*int64(memsize.MB), 2*time.Millisecond) // 1500 MiB/s
		EmitDownloadPerformance(stats, TORRENT_LEECH, 3*int64(memsize.MB), 1*time.Hour)        // 0.000833333333 MiB/s

		snapshot := stats.Snapshot()
		histograms := snapshot.Histograms()

		downloadThroughputXsmallKey := "p2p_leech_throughput+size=0B-5MiB"
		downloadThroughputXsmall, ok := histograms[downloadThroughputXsmallKey]
		require.True(ok)
		require.Equal(int64(1), downloadThroughputXsmall.Values()[math.MaxFloat64])
		require.Equal(int64(1), downloadThroughputXsmall.Values()[0.1])
	})
}
