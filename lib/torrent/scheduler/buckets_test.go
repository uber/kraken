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
	"testing"
	"time"

	"github.com/uber/kraken/utils/memsize"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestGetBucket(t *testing.T) {
	tests := []struct {
		desc     string
		size     uint64
		expected string
	}{
		{"below min", memsize.MB, "xsmall"},
		{"above max", 30 * memsize.GB, "xxlarge"},
		{"between buckets", 3 * memsize.GB, "large"},
		{"exact bucket", 1 * memsize.GB, "medium"},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require.Equal(t, test.expected, getBucket(test.size).sizeTag)
		})
	}
}

func TestBucketAddRange(t *testing.T) {
	tests := []struct {
		desc               string
		start, stop, width time.Duration
		expected           []time.Duration
	}{
		{"normal", 1, 4, 1, []time.Duration{1, 2, 3}},
		{"single", 1, 4, 5, []time.Duration{1}},
		{"none", 4, 1, 1, nil},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			b := newBucket("test", 0)
			b.addRange(test.start, test.stop, test.width)
			require.Equal(t, tally.DurationBuckets(test.expected), b.durations)
		})
	}
}
