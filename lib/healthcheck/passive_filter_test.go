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
package healthcheck

import (
	"testing"
	"time"

	"github.com/uber/kraken/utils/stringset"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func TestPassiveFilterUnhealthy(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()

	f := NewPassiveFilter(
		PassiveFilterConfig{Fails: 3, FailTimeout: 10 * time.Second},
		clk)

	x := "x:80"
	y := "y:80"
	s := stringset.New(x, y)

	require.Equal(stringset.New(x, y), f.Run(s))

	for i := 0; i < 3; i++ {
		f.Failed(x)
	}

	require.Equal(stringset.New(y), f.Run(s))
}

func TestPassiveFilterFailTimeout(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()

	f := NewPassiveFilter(
		PassiveFilterConfig{Fails: 3, FailTimeout: 10 * time.Second},
		clk)

	x := "x:80"
	y := "y:80"
	s := stringset.New(x, y)

	f.Failed(x)
	f.Failed(x)

	clk.Add(11 * time.Second)

	f.Failed(x)

	require.Equal(stringset.New(x, y), f.Run(s))
}

func TestPassiveFilterFailTimeoutAfterUnhealthy(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()

	f := NewPassiveFilter(
		PassiveFilterConfig{Fails: 3, FailTimeout: 10 * time.Second},
		clk)

	x := "x:80"
	y := "y:80"
	s := stringset.New(x, y)

	for i := 0; i < 3; i++ {
		f.Failed(x)
	}

	require.Equal(stringset.New(y), f.Run(s))

	clk.Add(5 * time.Second)

	// Stil unhealthy...
	require.Equal(stringset.New(y), f.Run(s))

	clk.Add(6 * time.Second)

	// Timeout has now elapsed, host is healthy again.
	require.Equal(stringset.New(x, y), f.Run(s))
}
