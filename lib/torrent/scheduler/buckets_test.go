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
		"exact bucket":    {1 * memsize.GB, _medium},
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

func TestEmitBlobDownloadPerformance(t *testing.T) {
	t.Run("latency", func(t *testing.T) {
		require := require.New(t)
		stats := tally.NewTestScope("", nil)

		emitBlobDownloadPerformance(stats, 3*int64(memsize.MB), 220*time.Millisecond)
		emitBlobDownloadPerformance(stats, 10*int64(memsize.GB), 20*time.Minute)

		snapshot := stats.Snapshot()
		histograms := snapshot.Histograms()
		downloadTimeXsmallKey := "download_time+size=0B-100MiB,version=4"
		downloadTimeXsmall, ok := histograms[downloadTimeXsmallKey]
		require.True(ok)
		require.Equal(int64(1), downloadTimeXsmall.Durations()[500*time.Millisecond])

		downloadTimeXXlargeKey := "download_time+size=10GiB+,version=4"
		downloadTimeXXlarge, ok := histograms[downloadTimeXXlargeKey]
		require.True(ok)
		require.Equal(int64(1), downloadTimeXXlarge.Durations()[1260*time.Second])
	})

	t.Run("throughput", func(t *testing.T) {
		require := require.New(t)
		stats := tally.NewTestScope("", nil)

		emitBlobDownloadPerformance(stats, 3*int64(memsize.MB), 2*time.Second)   //1.5 MiB/s
		emitBlobDownloadPerformance(stats, 10*int64(memsize.GB), 20*time.Minute) // 8.53 MiB/s

		snapshot := stats.Snapshot()
		histograms := snapshot.Histograms()

		downloadThroughputXsmallKey := "download_throughput+size=0B-100MiB"
		downloadThroughputXsmall, ok := histograms[downloadThroughputXsmallKey]
		require.True(ok)
		require.Equal(int64(1), downloadThroughputXsmall.Values()[2])

		downloadThroughputXXlargeKey := "download_throughput+size=10GiB+"
		downloadThroughputXXlarge, ok := histograms[downloadThroughputXXlargeKey]
		require.True(ok)
		require.Equal(int64(1), downloadThroughputXXlarge.Values()[9])
	})
}
