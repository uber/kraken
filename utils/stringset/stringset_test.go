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
package stringset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSample(t *testing.T) {
	s := New("a", "b", "c")

	for _, n := range []int{0, 2, 3, 4} {
		sample := s.Sample(n)
		wantLen := n
		if wantLen > len(s) {
			wantLen = len(s)
		}
		require.Len(t, sample, wantLen)
		for value := range sample {
			require.True(t, s.Has(value), "Sample(%d) returned value %q outside the source set", n, value)
		}
	}
}
