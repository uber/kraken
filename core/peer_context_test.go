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
package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/utils/randutil"
)

func TestNewPeerContext(t *testing.T) {
	require := require.New(t)

	p := PeerContextFixture()

	_, err := NewPeerID(p.PeerID.String())
	require.NoError(err)
	require.False(p.Origin)
}

func TestNewOriginPeerContext(t *testing.T) {
	require := require.New(t)

	p := OriginContextFixture()

	_, err := NewPeerID(p.PeerID.String())
	require.NoError(err)
	require.True(p.Origin)
}

func TestNewOriginPeerContextErrors(t *testing.T) {
	t.Run("empty ip", func(t *testing.T) {
		require := require.New(t)

		_, err := NewPeerContext(
			RandomPeerIDFactory, "zone1", "test01-zone1", "", randutil.Port(), false)
		require.Error(err)
	})

	t.Run("zero port", func(t *testing.T) {
		require := require.New(t)

		_, err := NewPeerContext(
			RandomPeerIDFactory, "zone1", "test01-zone1", randutil.IP(), 0, false)
		require.Error(err)
	})

	t.Run("invalid factory", func(t *testing.T) {
		require := require.New(t)

		_, err := NewPeerContext(
			"invalid", "zone1", "test01-zone1", randutil.IP(), randutil.Port(), false)
		require.Error(err)
	})
}
