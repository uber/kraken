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
	_xsmall, _small, _medium, _large, _xlarge, _xxlarge = "0B - 100MiB", "100MiB-1GB", "1GiB-2GiB", "2GiB-5GiB", "5GiB-10GiB", "10GiB+"
)

var _buckets tally.DurationBuckets

var (
	_sizeBoundaries = []uint64{0, 100 * memsize.MB, memsize.GB, 2 * memsize.GB, 5 * memsize.GB, 10 * memsize.GB}
	_sizeTags       = []string{_xsmall, _small, _medium, _large, _xlarge, _xxlarge}
)

func createBucketBounderies(start, stop, width time.Duration) []time.Duration {
	var buckets []time.Duration
	for cur := start; cur <= stop; cur += width {
		buckets = append(buckets, cur)
	}
	return buckets
}

func init() {
	_buckets = []time.Duration{
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		7 * time.Second,
		12 * time.Second,
		15 * time.Second,
	}
	_buckets = append(_buckets, createBucketBounderies(20*time.Second, time.Minute, 5*time.Second)...)
	_buckets = append(_buckets, createBucketBounderies(time.Minute+10*time.Second, 2*time.Minute, 10*time.Second)...)
	_buckets = append(_buckets, createBucketBounderies(2*time.Minute+20*time.Second, 4*time.Minute, 20*time.Second)...)
	_buckets = append(_buckets, createBucketBounderies(4*time.Minute+30*time.Second, 8*time.Minute, 30*time.Second)...)
	_buckets = append(_buckets, createBucketBounderies(9*time.Minute, 15*time.Minute, time.Minute)...)
	_buckets = append(_buckets, createBucketBounderies(17*time.Minute, 25*time.Minute, 2*time.Minute)...)
	_buckets = append(_buckets, createBucketBounderies(30*time.Minute, 40*time.Minute, 5*time.Minute)...)
	_buckets = append(_buckets, createBucketBounderies(45*time.Minute, 60*time.Minute, 5*time.Minute)...)

	// Sanity check to ensure buckets are sorted.
	for i := 0; i < len(_buckets)-1; i++ {
		if _buckets[i] >= _buckets[i+1] {
			panic("buckets are not sorted properly")
		}
	}
}

func getSizeTag(sizeBytes uint64) string {
	for i := len(_sizeBoundaries) - 1; i >= 0; i-- {
		if sizeBytes >= _sizeBoundaries[i] {
			return _sizeTags[i]
		}
	}
	return _sizeTags[0]
}

func emitDownloadTime(stats tally.Scope, sizeBytes int64, t time.Duration) {
	sizeTag := getSizeTag(uint64(sizeBytes))

	stats.Tagged(map[string]string{
		"size":    sizeTag,
		"version": "4",
	}).Histogram("download_time", _buckets).RecordDuration(t)
}
