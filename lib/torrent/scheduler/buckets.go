// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scheduler

import (
	"time"

	"github.com/uber/kraken/utils/memsize"
	"github.com/uber-go/tally"
)

type bucket struct {
	sizeTag   string
	min       uint64
	durations tally.DurationBuckets
}

func newBucket(sizeTag string, min uint64) *bucket {
	return &bucket{sizeTag, min, nil}
}

func (b *bucket) addRange(start, stop, width time.Duration) {
	cur := start
	for cur < stop {
		b.durations = append(b.durations, cur)
		cur += width
	}
}

var _buckets []*bucket

func init() {
	xs := newBucket("xsmall", 0)
	xs.addRange(250*time.Millisecond, time.Second, 250*time.Millisecond)
	xs.addRange(time.Second, 20*time.Second, time.Second)
	xs.addRange(20*time.Second, time.Minute, 5*time.Second)
	xs.addRange(time.Minute, 5*time.Minute, time.Minute)
	xs.addRange(5*time.Minute, 30*time.Minute, 5*time.Minute)
	_buckets = append(_buckets, xs)

	s := newBucket("small", 100*memsize.MB)
	s.addRange(time.Second, 30*time.Second, time.Second)
	s.addRange(30*time.Second, time.Minute, 5*time.Second)
	s.addRange(time.Minute, 5*time.Minute, time.Minute)
	s.addRange(5*time.Minute, 30*time.Minute, 5*time.Minute)
	_buckets = append(_buckets, s)

	m := newBucket("medium", memsize.GB)
	m.addRange(2*time.Second, time.Minute, 2*time.Second)
	m.addRange(time.Minute, 2*time.Minute, 5*time.Second)
	m.addRange(2*time.Minute, 5*time.Minute, 30*time.Second)
	m.addRange(5*time.Minute, 10*time.Minute, time.Minute)
	m.addRange(10*time.Minute, 30*time.Minute, 5*time.Minute)
	_buckets = append(_buckets, m)

	l := newBucket("large", 2*memsize.GB)
	l.addRange(5*time.Second, 3*time.Minute, 5*time.Second)
	l.addRange(3*time.Minute, 5*time.Minute, 30*time.Second)
	l.addRange(5*time.Minute, 10*time.Minute, time.Minute)
	l.addRange(10*time.Minute, 30*time.Minute, 5*time.Minute)
	_buckets = append(_buckets, l)

	xl := newBucket("xlarge", 5*memsize.GB)
	xl.addRange(10*time.Second, 5*time.Minute, 10*time.Second)
	xl.addRange(5*time.Minute, 10*time.Minute, 30*time.Second)
	xl.addRange(10*time.Minute, 30*time.Minute, time.Minute)
	_buckets = append(_buckets, xl)

	xxl := newBucket("xxlarge", 10*memsize.GB)
	xxl.addRange(15*time.Second, 10*time.Minute, 15*time.Second)
	xxl.addRange(10*time.Minute, 15*time.Minute, 30*time.Second)
	xxl.addRange(15*time.Minute, 30*time.Minute, time.Minute)
	_buckets = append(_buckets, xxl)

	// Sanity check to ensure buckets are sorted.
	for i := 0; i < len(_buckets)-1; i++ {
		if _buckets[i].min >= _buckets[i+1].min {
			panic("buckets are not sorted properly")
		}
	}
}

// getBucket selects the largest bucket size fits into.
func getBucket(size uint64) (b *bucket) {
	for i := len(_buckets) - 1; i >= 0; i-- {
		b = _buckets[i]
		if size >= b.min {
			break
		}
	}
	return b
}

func recordDownloadTime(stats tally.Scope, size int64, t time.Duration) {
	b := getBucket(uint64(size))
	stats.Tagged(map[string]string{
		"size":    b.sizeTag,
		"version": "3",
	}).Histogram("download_time", b.durations).RecordDuration(t)
}
