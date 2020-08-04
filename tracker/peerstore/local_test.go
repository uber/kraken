// Copyright (c) 2016-2020 Uber Technologies, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
)

func TestLocalStoreExpiration(t *testing.T) {
	now := time.Date(2019, time.November, 1, 1, 0, 0, 0, time.UTC)
	clk := clock.NewMock()
	clk.Set(now)

	s := NewLocalStore(LocalConfig{TTL: 10 * time.Minute}, clk)
	defer s.Close()

	h1 := core.InfoHashFixture()

	// No peers initially.

	peers, err := s.GetPeers(h1, 0)
	require.NoError(t, err)
	require.Empty(t, peers)

	peers, err = s.GetPeers(h1, 1)
	require.NoError(t, err)
	require.Empty(t, peers)

	p1 := core.PeerInfoFixture()
	require.NoError(t, s.UpdatePeer(h1, p1))

	p2 := core.PeerInfoFixture()
	require.NoError(t, s.UpdatePeer(h1, p2))

	// Two peers with some different n values.

	peers, err = s.GetPeers(h1, 2)
	require.NoError(t, err)
	require.ElementsMatch(t, []*core.PeerInfo{p1, p2}, peers)

	peers, err = s.GetPeers(h1, 50)
	require.ElementsMatch(t, []*core.PeerInfo{p1, p2}, peers)

	peers, err = s.GetPeers(h1, 1)
	require.NoError(t, err)
	require.Len(t, peers, 1)

	clk.Add(5 * time.Minute)

	p3 := core.PeerInfoFixture()
	require.NoError(t, s.UpdatePeer(h1, p3))

	// Manually triggered for testing purposes. Nothing has expired, so
	// should be a noop.
	s.cleanupExpiredPeerEntries()
	s.cleanupExpiredPeerGroups()

	peers, err = s.GetPeers(h1, 3)
	require.NoError(t, err)
	require.ElementsMatch(t, []*core.PeerInfo{p1, p2, p3}, peers)

	// Update existing peer.
	p3.Complete = true
	require.NoError(t, s.UpdatePeer(h1, p3))

	peers, err = s.GetPeers(h1, 3)
	require.NoError(t, err)
	require.ElementsMatch(t, []*core.PeerInfo{p1, p2, p3}, peers)

	clk.Add(5*time.Minute + 1)

	// Manually triggered for testing purposes.
	s.cleanupExpiredPeerEntries()

	// p1 and p2 are now expired.
	peers, err = s.GetPeers(h1, 3)
	require.NoError(t, err)
	require.ElementsMatch(t, []*core.PeerInfo{p3}, peers)

	clk.Add(5*time.Minute + 1)

	// Manually triggered for testing purposes.
	s.cleanupExpiredPeerEntries()

	// p3 is now expired.
	peers, err = s.GetPeers(h1, 1)
	require.NoError(t, err)
	require.Empty(t, peers)

	// Unfortunately we must reach into the LocalStore's private state
	// to determine whether cleanup actually occurred.
	require.Contains(t, s.peerGroups, h1)
	s.cleanupExpiredPeerGroups()
	require.NotContains(t, s.peerGroups, h1)
}

func TestLocalStoreConcurrency(t *testing.T) {
	s := NewLocalStore(LocalConfig{TTL: time.Millisecond}, clock.New())
	defer s.Close()

	hashes := []core.InfoHash{
		core.InfoHashFixture(),
		core.InfoHashFixture(),
		core.InfoHashFixture(),
	}

	// We don't care what the results are, we just want to trigger any race
	// conditions.
	var wg sync.WaitGroup
	for n := 0; n < 1000; n++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for _, h := range hashes {
				require.NoError(t, s.UpdatePeer(h, core.PeerInfoFixture()))
			}
		}()
		go func() {
			defer wg.Done()
			for _, h := range hashes {
				peers, err := s.GetPeers(h, 10)
				require.NoError(t, err)
				require.True(t, len(peers) <= 10)
			}
		}()
	}
	wg.Wait()
}
