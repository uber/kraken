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
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/utils/memsize"
)

const (
	_xsmall, _small, _medium, _large, _xlarge, _xxlarge = "0B-100MiB", "100MiB-1GB", "1GiB-2GiB", "2GiB-5GiB", "5GiB-10GiB", "10GiB+"
)

var (
	_sizeBoundaries         = []uint64{0, 100 * memsize.MB, memsize.GB, 2 * memsize.GB, 5 * memsize.GB, 10 * memsize.GB}
	_sizeTags               = []string{_xsmall, _small, _medium, _large, _xlarge, _xxlarge}
	_downloadLatencyBuckets = append(
		[]time.Duration{
			0,
			500 * time.Millisecond,
			1 * time.Second,
			2 * time.Second,
			4 * time.Second,
			7 * time.Second,
			12 * time.Second,
			15 * time.Second,
		},
		append(
			tally.MustMakeLinearDurationBuckets(20*time.Second, 5*time.Second, 9),
			append(
				tally.MustMakeLinearDurationBuckets(70*time.Second, 10*time.Second, 6),
				append(
					tally.MustMakeLinearDurationBuckets(140*time.Second, 20*time.Second, 6),
					append(
						tally.MustMakeLinearDurationBuckets(270*time.Second, 30*time.Second, 8),
						append(
							tally.MustMakeLinearDurationBuckets(540*time.Second, 60*time.Second, 7),
							append(
								tally.MustMakeLinearDurationBuckets(1020*time.Second, 120*time.Second, 5),
								append(
									tally.MustMakeLinearDurationBuckets(1800*time.Second, 300*time.Second, 3),
									tally.MustMakeLinearDurationBuckets(2700*time.Second, 300*time.Second, 4)...,
								)...,
							)...,
						)...,
					)...,
				)...,
			)...,
		)...,
	)

	// In MiB/s.
	_downloadThroughputBuckets = append(
		tally.MustMakeLinearValueBuckets(0, 1, 10), // [0, 10)
		append(
			tally.MustMakeLinearValueBuckets(10, 2, 5), // [10, 20)
			append(
				tally.MustMakeLinearValueBuckets(20, 5, 6), // [20, 50)
				append(
					tally.MustMakeLinearValueBuckets(50, 10, 15),                // [50, 200)
					tally.MustMakeLinearValueBuckets(200, 50, 4)...)...)...)..., // [200, 400)
	)
)

func getSizeTag(sizeBytes uint64) string {
	for i := len(_sizeBoundaries) - 1; i >= 0; i-- {
		if sizeBytes >= _sizeBoundaries[i] {
			return _sizeTags[i]
		}
	}
	return _sizeTags[0]
}

// emitBlobDownloadPerformance emits metrics on the speed of a blob's download from the moment
// the blob was requested through Kraken's API to the moment it is fully downloaded.
func emitBlobDownloadPerformance(stats tally.Scope, sizeBytes int64, t time.Duration) {
	sizeTag := getSizeTag(uint64(sizeBytes))

	stats.Tagged(map[string]string{
		"size":    sizeTag,
		"version": "4",
	}).Histogram("download_time", tally.DurationBuckets(_downloadLatencyBuckets)).RecordDuration(t)

	mbPerSecond := (float64(sizeBytes) / (float64(memsize.MB))) / t.Seconds()
	stats.Tagged(map[string]string{
		"size": sizeTag,
	}).Histogram("download_throughput", _downloadThroughputBuckets).RecordValue(mbPerSecond)
}
