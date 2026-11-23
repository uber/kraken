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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromSliceAndNew(t *testing.T) {
	require := require.New(t)

	s := FromSlice([]string{"a", "b", "a"})
	require.Len(s, 2)
	require.True(s.Has("a"))
	require.True(s.Has("b"))
	require.True(Equal(s, New("b", "a")))
	require.Empty(New())
}

func TestAddRemoveHas(t *testing.T) {
	require := require.New(t)

	s := New()
	require.False(s.Has("a"))

	s.Add("a")
	s.Add("a")
	require.True(s.Has("a"))
	require.Len(s, 1)

	s.Remove("a")
	s.Remove("missing")
	require.False(s.Has("a"))
	require.Empty(s)
}

func TestSub(t *testing.T) {
	require := require.New(t)

	s := New("a", "b", "c")
	result := s.Sub(New("b", "d"))

	require.True(Equal(New("a", "c"), result))
	require.True(Equal(New("a", "b", "c"), s))
}

func TestToSlice(t *testing.T) {
	require := require.New(t)

	xs := New("a", "b", "c").ToSlice()
	sort.Strings(xs)
	require.Equal([]string{"a", "b", "c"}, xs)
	require.Empty(New().ToSlice())
}

func TestEqual(t *testing.T) {
	tests := []struct {
		desc     string
		s1       Set
		s2       Set
		expected bool
	}{
		{"both empty", New(), New(), true},
		{"same elements", New("a", "b"), New("b", "a"), true},
		{"different sizes", New("a"), New("a", "b"), false},
		{"same size, different elements", New("a", "b"), New("a", "c"), false},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require.Equal(t, test.expected, Equal(test.s1, test.s2))
			require.Equal(t, test.expected, Equal(test.s2, test.s1))
		})
	}
}

func TestCopy(t *testing.T) {
	require := require.New(t)

	s := New("a", "b")
	c := s.Copy()
	require.True(Equal(s, c))

	c.Add("c")
	require.False(s.Has("c"))
}

func TestRandom(t *testing.T) {
	require := require.New(t)

	s := New("a", "b", "c")
	x, err := s.Random()
	require.NoError(err)
	require.True(s.Has(x))

	_, err = New().Random()
	require.Error(err)
}

func TestSample(t *testing.T) {
	tests := []struct {
		desc     string
		set      Set
		n        int
		expected int
	}{
		{"fewer than the set", New("a", "b", "c", "d", "e"), 3, 3},
		{"as many as the set", New("a", "b", "c"), 3, 3},
		{"more than the set", New("a", "b"), 3, 2},
		{"zero", New("a", "b"), 0, 0},
		{"empty set", New(), 3, 0},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			original := test.set.Copy()
			sample := test.set.Sample(test.n)

			require.Len(sample, test.expected)
			for x := range sample {
				require.True(test.set.Has(x))
			}
			require.True(Equal(original, test.set))
		})
	}
}
