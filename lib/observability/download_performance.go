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
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/utils/memsize"
)

const (
	_xsmall, _small, _medium, _large, _xlarge, _xxlarge = "0B-5MiB", "5MiB-100MiB", "100MiB-1GiB", "1GiB-5GiB", "5GiB-10GiB", "10GiBplus"
)

var (
	_sizeBoundaries = []uint64{0, 5 * memsize.MB, 100 * memsize.MB, memsize.GB, 5 * memsize.GB, 10 * memsize.GB}
	_sizeTags       = []string{_xsmall, _small, _medium, _large, _xlarge, _xxlarge}

	_downloadLatencyBuckets tally.DurationBuckets
	// In MiB/s.
	_downloadThroughputBuckets tally.ValueBuckets
)

func init() {
	_downloadLatencyBuckets = append(_downloadLatencyBuckets, 0,
		500*time.Millisecond,
		1*time.Second,
		2*time.Second,
		4*time.Second,
		7*time.Second,
		12*time.Second,
		15*time.Second)
	_downloadLatencyBuckets = append(_downloadLatencyBuckets, tally.MustMakeLinearDurationBuckets(20*time.Second, 5*time.Second, 9)...)
	_downloadLatencyBuckets = append(_downloadLatencyBuckets, tally.MustMakeLinearDurationBuckets(70*time.Second, 10*time.Second, 6)...)
	_downloadLatencyBuckets = append(_downloadLatencyBuckets, tally.MustMakeLinearDurationBuckets(140*time.Second, 20*time.Second, 6)...)
	_downloadLatencyBuckets = append(_downloadLatencyBuckets, tally.MustMakeLinearDurationBuckets(270*time.Second, 30*time.Second, 8)...)
	_downloadLatencyBuckets = append(_downloadLatencyBuckets, tally.MustMakeLinearDurationBuckets(540*time.Second, 60*time.Second, 7)...)
	_downloadLatencyBuckets = append(_downloadLatencyBuckets, tally.MustMakeLinearDurationBuckets(1020*time.Second, 120*time.Second, 5)...)
	_downloadLatencyBuckets = append(_downloadLatencyBuckets, tally.MustMakeLinearDurationBuckets(1800*time.Second, 300*time.Second, 3)...)
	_downloadLatencyBuckets = append(_downloadLatencyBuckets, tally.MustMakeLinearDurationBuckets(2700*time.Second, 300*time.Second, 4)...)

	_downloadThroughputBuckets = append(_downloadThroughputBuckets, tally.MustMakeLinearValueBuckets(0, 0.1, 10)...)  // [0, 1)
	_downloadThroughputBuckets = append(_downloadThroughputBuckets, tally.MustMakeLinearValueBuckets(1, 0.5, 4)...)   // [1, 3)
	_downloadThroughputBuckets = append(_downloadThroughputBuckets, tally.MustMakeLinearValueBuckets(3, 1, 7)...)     // [3, 10)
	_downloadThroughputBuckets = append(_downloadThroughputBuckets, tally.MustMakeLinearValueBuckets(10, 2, 10)...)   // [10, 30)
	_downloadThroughputBuckets = append(_downloadThroughputBuckets, tally.MustMakeLinearValueBuckets(30, 5, 4)...)    // [30, 50)
	_downloadThroughputBuckets = append(_downloadThroughputBuckets, tally.MustMakeLinearValueBuckets(50, 10, 5)...)   // [50, 100)
	_downloadThroughputBuckets = append(_downloadThroughputBuckets, tally.MustMakeLinearValueBuckets(100, 50, 4)...)  // [100, 300)
	_downloadThroughputBuckets = append(_downloadThroughputBuckets, tally.MustMakeLinearValueBuckets(300, 100, 7)...) // [100, 800)
}

func getSizeTag(sizeBytes uint64) string {
	for i := len(_sizeBoundaries) - 1; i >= 0; i-- {
		if sizeBytes >= _sizeBoundaries[i] {
			return _sizeTags[i]
		}
	}
	return _sizeTags[0]
}

type DownloadType string

const (
	// Measures the end-to-end download of a torrent (blob), including the GetMetainfo call.
	TORRENT_DOWNLOAD DownloadType = "TORRENT_DOWNLOAD"
	// Measures the torrent leeching throughput from peers. EXCLUDES any other parts of the download, e.g. the GetMetainfo call.
	TORRENT_LEECH DownloadType = "TORRENT_LEECH"
	// Measures the client-side GetMetainfo call performance.
	METAINFO_DOWNLOAD DownloadType = "METAINFO_DOWNLOAD"
)

// EmitDownloadPerformance emits metrics (usually latency and throughput) on the download performance of a blob.
// Check the respective [DownloadType] for more context.
func EmitDownloadPerformance(stats tally.Scope, downloadType DownloadType, sizeBytes int64, t time.Duration) {
	sizeTag := getSizeTag(uint64(sizeBytes))
	mbPerSecond := (float64(sizeBytes) / (float64(memsize.MB))) / t.Seconds()

	switch downloadType {
	case TORRENT_DOWNLOAD:
		emitBlobDownloadPerformance(stats, mbPerSecond, sizeTag, t)
	case METAINFO_DOWNLOAD:
		emitMetainfoDownloadPerformance(stats, mbPerSecond, sizeTag, t)
	case TORRENT_LEECH:
	}
}

func emitBlobDownloadPerformance(stats tally.Scope, mbPerSecond float64, sizeTag string, t time.Duration) {
	stats.Tagged(map[string]string{
		"size":    sizeTag,
		"version": "4",
	}).Histogram("download_time", _downloadLatencyBuckets).RecordDuration(t)

	stats.Tagged(map[string]string{
		"size":    sizeTag,
		"version": "2",
	}).Histogram("download_throughput", _downloadThroughputBuckets).RecordValue(mbPerSecond)
}

func emitTorrentLeechingPerformance(stats tally.Scope, mbPerSecond float64, sizeTag string) {
	stats.Tagged(map[string]string{
		"size": sizeTag,
	}).Histogram("p2p_leeching_throughput", _downloadThroughputBuckets).RecordValue(mbPerSecond)
}

func emitMetainfoDownloadPerformance(stats tally.Scope, mbPerSecond float64, sizeTag string, t time.Duration) {
	// Metrics are tagged by torrent_size, as origin needs to download the blob from storage (e.g. GCS) to
	// calculate its metainfo, before returning it to tracker, which in turn, returns it to agent. Thus, even if
	// metainfo itself is <1 KiB, its download may take 10s of minutes on huge blobs.

	stats.Tagged(map[string]string{
		"torrent_size": sizeTag,
	}).Histogram("metainfo_download_time", _downloadLatencyBuckets).RecordDuration(t)

	stats.Tagged(map[string]string{
		"torrent_size": sizeTag,
		"version":      "2",
	}).Histogram("metainfo_download_throughput", _downloadThroughputBuckets).RecordValue(mbPerSecond)
}
