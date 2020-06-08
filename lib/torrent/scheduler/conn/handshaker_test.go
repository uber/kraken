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
package conn

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/gen/go/proto/p2p"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/utils/bitsetutil"
)

func TestHandshakerSetsConnFieldsProperly(t *testing.T) {
	require := require.New(t)

	l1, err := net.Listen("tcp", "localhost:0")
	require.NoError(err)
	defer l1.Close()

	config := ConfigFixture()
	namespace := core.TagFixture()
	h1 := HandshakerFixture(config)
	h2 := HandshakerFixture(config)

	info := storage.TorrentInfoFixture(4, 1)
	emptyRemoteBitfields := make(RemoteBitfields)
	remoteBitfields := RemoteBitfields{
		core.PeerIDFixture(): bitsetutil.FromBools(true, false),
		core.PeerIDFixture(): bitsetutil.FromBools(false, true),
	}

	var wg sync.WaitGroup

	start := time.Now()

	wg.Add(1)
	go func() {
		defer wg.Done()

		nc, err := l1.Accept()
		require.NoError(err)

		pc, err := h1.Accept(nc)
		require.NoError(err)
		require.Equal(h2.peerID, pc.PeerID())
		require.Equal(info.Digest(), pc.Digest())
		require.Equal(info.InfoHash(), pc.InfoHash())
		require.Equal(info.Bitfield(), pc.Bitfield())
		require.Equal(namespace, pc.Namespace())

		c, err := h1.Establish(pc, info, remoteBitfields)
		require.NoError(err)
		require.Equal(h2.peerID, c.PeerID())
		require.Equal(info.InfoHash(), c.InfoHash())
		require.True(c.CreatedAt().After(start))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		r, err := h2.Initialize(h1.peerID, l1.Addr().String(), info, emptyRemoteBitfields, namespace)
		require.NoError(err)
		require.Equal(h1.peerID, r.Conn.PeerID())
		require.Equal(info.InfoHash(), r.Conn.InfoHash())
		require.True(r.Conn.CreatedAt().After(start))
		require.Equal(info.Bitfield(), r.Bitfield)
		require.Equal(remoteBitfields, r.RemoteBitfields)
	}()

	wg.Wait()
}

func TestHandshakerHandlesEmptyBitfield(t *testing.T) {
	require := require.New(t)

	l1, err := net.Listen("tcp", "localhost:0")
	require.NoError(err)
	defer l1.Close()

	config := ConfigFixture()
	h1 := HandshakerFixture(config)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		nc, err := l1.Accept()
		require.NoError(err)

		_, err = h1.Accept(nc)
		require.Error(err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		var msg p2p.Message

		nc, err := net.DialTimeout("tcp", l1.Addr().String(), config.HandshakeTimeout)
		require.NoError(err)
		err = sendMessage(nc, &msg)
		require.NoError(err)
	}()

	wg.Wait()
}
