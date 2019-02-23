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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/stringset"
)

func TestAddrHashPeerIDFactory(t *testing.T) {
	require := require.New(t)

	ip := randutil.IP()
	port := randutil.Port()
	p1, err := AddrHashPeerIDFactory.GeneratePeerID(ip, port)
	require.NoError(err)
	p2, err := AddrHashPeerIDFactory.GeneratePeerID(ip, port)
	require.NoError(err)
	require.Equal(p1.String(), p2.String())
}

func TestNewPeerIDErrors(t *testing.T) {
	tests := []struct {
		desc  string
		input string
	}{
		{"empty", ""},
		{"invalid hex", "invalid"},
		{"too short", "beef"},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			_, err := NewPeerID(test.input)
			require.Error(t, err)
		})
	}
}

func TestHashedPeerID(t *testing.T) {
	require := require.New(t)

	n := 50

	ids := make(stringset.Set)
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("%s:%d", randutil.IP(), randutil.Port())
		peerID, err := HashedPeerID(addr)
		require.NoError(err)
		ids.Add(peerID.String())
	}

	// None of the hashes should conflict.
	require.Len(ids, n)
}

func TestHashedPeerIDReturnsErrOnEmpty(t *testing.T) {
	require := require.New(t)

	_, err := HashedPeerID("")
	require.Error(err)
}

func TestPeerIDCompare(t *testing.T) {
	require := require.New(t)

	peer1 := PeerIDFixture()
	peer2 := PeerIDFixture()
	if peer1.String() < peer2.String() {
		require.True(peer1.LessThan(peer2))
	} else if peer1.String() > peer2.String() {
		require.True(peer2.LessThan(peer1))
	}
}
