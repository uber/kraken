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
package syncutil

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountersIncrement(t *testing.T) {
	require := require.New(t)

	c := NewCounters(10)

	wg := sync.WaitGroup{}
	for k := 0; k < 100; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			c.Increment(k % c.Len())
		}(k)
	}
	wg.Wait()

	for k := 0; k < c.Len(); k++ {
		require.Equal(10, c.Get(k))
	}
}

func TestCountersDecrement(t *testing.T) {
	require := require.New(t)

	c := NewCounters(10)
	for k := 0; k < c.Len(); k++ {
		c.Set(k, 10)
	}

	wg := sync.WaitGroup{}
	for k := 0; k < 100; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			c.Decrement(k % c.Len())
		}(k)
	}
	wg.Wait()

	for k := 0; k < c.Len(); k++ {
		require.Equal(0, c.Get(k))
	}
}

func TestCountersSet(t *testing.T) {
	require := require.New(t)

	c := NewCounters(10)

	wg := sync.WaitGroup{}
	for k := 0; k < 100; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			c.Set(k%c.Len(), -1)
		}(k)
	}
	wg.Wait()

	for k := 0; k < c.Len(); k++ {
		require.Equal(-1, c.Get(k))
	}
}
