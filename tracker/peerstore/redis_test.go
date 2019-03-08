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
package peerstore

import (
	"testing"
	"time"

	"github.com/uber/kraken/core"

	"github.com/alicebob/miniredis"
	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func redisConfigFixture() RedisConfig {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	return RedisConfig{
		Addr:              s.Addr(),
		PeerSetWindowSize: 30 * time.Second,
		MaxPeerSetWindows: 4,
	}
}

func TestRedisStoreGetPeersPopulatesPeerInfoFields(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	s, err := NewRedisStore(config, clock.New())
	require.NoError(err)

	h := core.InfoHashFixture()

	p := core.PeerInfoFixture()
	p.Complete = true

	require.NoError(s.UpdatePeer(h, p))

	peers, err := s.GetPeers(h, 1)
	require.NoError(err)
	require.Equal(peers, []*core.PeerInfo{p})
}

func TestRedisStoreGetPeersFromMultipleWindows(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()
	config.PeerSetWindowSize = 10 * time.Second
	config.MaxPeerSetWindows = 3

	clk := clock.NewMock()
	clk.Set(time.Now())

	s, err := NewRedisStore(config, clk)
	require.NoError(err)

	// Reset time to the beginning of a window.
	clk.Set(time.Unix(s.curPeerSetWindow(), 0))

	h := core.InfoHashFixture()

	// Each peer will be added on a different second to distribute them across
	// multiple windows.
	var peers []*core.PeerInfo
	for i := 0; i < int(config.PeerSetWindowSize.Seconds())*config.MaxPeerSetWindows; i++ {
		if i > 0 {
			clk.Add(time.Second)
		}
		p := core.PeerInfoFixture()
		peers = append(peers, p)
		require.NoError(s.UpdatePeer(h, p))
	}

	result, err := s.GetPeers(h, len(peers))
	require.NoError(err)
	require.Equal(core.SortedByPeerID(peers), core.SortedByPeerID(result))
}

func TestRedisStoreGetPeersLimit(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()
	config.PeerSetWindowSize = 10 * time.Second
	config.MaxPeerSetWindows = 3

	clk := clock.NewMock()
	clk.Set(time.Now())

	s, err := NewRedisStore(config, clk)
	require.NoError(err)

	// Reset time to the beginning of a window.
	clk.Set(time.Unix(s.curPeerSetWindow(), 0))

	h := core.InfoHashFixture()

	// Each peer will be added on a different second to distribute them across
	// multiple windows.
	for i := 0; i < 30; i++ {
		if i > 0 {
			clk.Add(time.Second)
		}
		require.NoError(s.UpdatePeer(h, core.PeerInfoFixture()))
	}

	// Request more peers than were added on a single window to ensure we obey the limit
	// across multiple windows.
	for i := 0; i < 100; i++ {
		result, err := s.GetPeers(h, 15)
		require.NoError(err)
		require.Len(result, 15)
	}
}

func TestRedisStoreGetPeersCollapsesCompleteBits(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	s, err := NewRedisStore(config, clock.New())
	require.NoError(err)

	h := core.InfoHashFixture()
	p := core.PeerInfoFixture()

	require.NoError(s.UpdatePeer(h, p))

	peers, err := s.GetPeers(h, 2)
	require.NoError(err)
	require.Len(peers, 1)
	require.False(peers[0].Complete)

	p.Complete = true
	require.NoError(s.UpdatePeer(h, p))

	peers, err = s.GetPeers(h, 2)
	require.NoError(err)
	require.Len(peers, 1)
	require.True(peers[0].Complete)
}

func TestRedisStorePeerExpiration(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()
	config.PeerSetWindowSize = time.Second
	config.MaxPeerSetWindows = 2

	s, err := NewRedisStore(config, clock.New())
	require.NoError(err)

	h := core.InfoHashFixture()
	p := core.PeerInfoFixture()

	require.NoError(s.UpdatePeer(h, p))

	result, err := s.GetPeers(h, 1)
	require.NoError(err)
	require.Len(result, 1)

	time.Sleep(3 * time.Second)

	result, err = s.GetPeers(h, 1)
	require.NoError(err)
	require.Empty(result)
}
