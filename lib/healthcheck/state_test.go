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

	"github.com/uber/kraken/utils/stringset"

	"github.com/stretchr/testify/require"
)

func TestStateHealthTransition(t *testing.T) {
	require := require.New(t)

	s := newState(FilterConfig{Fails: 3, Passes: 2})

	addr := "foo:80"

	s.passed(addr) // +1
	require.Empty(s.getHealthy())

	s.passed(addr) // +2 (healthy)
	require.Equal(stringset.New(addr), s.getHealthy())

	s.passed(addr) // +2 (noop)
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -1
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -2
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -3 (unhealthy)
	require.Empty(s.getHealthy())

	s.failed(addr) // -3 (noop)
	require.Empty(s.getHealthy())

	s.passed(addr) // +1
	require.Empty(s.getHealthy())

	s.passed(addr) // +2 (healthy)
	require.Equal(stringset.New(addr), s.getHealthy())

	s.passed(addr) // +2 (noop)
	require.Equal(stringset.New(addr), s.getHealthy())
}

func TestStateHealthTrendResets(t *testing.T) {
	require := require.New(t)

	s := newState(FilterConfig{Fails: 3, Passes: 2})

	addr := "foo:80"

	s.passed(addr) // +1
	require.Empty(s.getHealthy())

	s.passed(addr) // +2 (healthy)
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -1
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -2
	require.Equal(stringset.New(addr), s.getHealthy())

	s.passed(addr) // +1 (resets)
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -1
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -2
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -3 (unhealthy)
	require.Empty(s.getHealthy())
}

func TestStateSync(t *testing.T) {
	require := require.New(t)

	s := newState(FilterConfig{Fails: 1, Passes: 1})

	addr1 := "foo:80"
	addr2 := "bar:80"

	s.sync(stringset.New(addr1, addr2))

	require.Equal(stringset.New(addr1, addr2), s.getHealthy())

	s.sync(stringset.New(addr1))

	require.Equal(stringset.New(addr1), s.getHealthy())
}
