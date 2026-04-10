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

func TestCreateBucketBounderies(t *testing.T) {
	tests := map[string]struct {
		start, stop, width time.Duration
		expected           []time.Duration
	}{
		"normal": {1, 4, 1, []time.Duration{1, 2, 3, 4}},
		"single": {1, 4, 5, []time.Duration{1}},
		"none":   {4, 1, 1, nil},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			res := createBucketBounderies(tt.start, tt.stop, tt.width)
			require.Equal(t, tt.expected, res)
		})
	}
}
