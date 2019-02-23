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
package originstore

import (
	"errors"
	"testing"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/utils/testutil"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const _testDNS = "test-origin-cluster-dns:80"

type storeMocks struct {
	ctrl     *gomock.Controller
	provider *mockblobclient.MockProvider
}

func newStoreMocks(t *testing.T) (*storeMocks, func()) {
	cleanup := &testutil.Cleanup{}

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	provider := mockblobclient.NewMockProvider(ctrl)

	return &storeMocks{ctrl, provider}, cleanup.Run
}

func (m *storeMocks) new(config Config, clk clock.Clock) Store {
	return New(config, clk, hostlist.Fixture(_testDNS), m.provider)
}

func (m *storeMocks) expectClient(addr string) *mockblobclient.MockClient {
	client := mockblobclient.NewMockClient(m.ctrl)
	m.provider.EXPECT().Provide(addr).Return(client)
	return client
}

func originViews(n int) (octxs []core.PeerContext, addrs []string, pinfos []*core.PeerInfo) {
	for i := 0; i < n; i++ {
		octx := core.OriginContextFixture()
		octxs = append(octxs, octx)
		addrs = append(addrs, octx.IP)
		pinfos = append(pinfos, core.PeerInfoFromContext(octx, true))
	}
	return
}

func TestStoreGetOrigins(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(Config{}, clock.New())

	d := core.DigestFixture()
	octxs, addrs, pinfos := originViews(3)

	dnsClient := mocks.expectClient(_testDNS)
	dnsClient.EXPECT().Locations(d).Return(addrs, nil)

	for _, octx := range octxs {
		client := mocks.expectClient(octx.IP)
		client.EXPECT().GetPeerContext().Return(octx, nil)
	}

	// Ensure caching.
	for i := 0; i < 100; i++ {
		result, err := store.GetOrigins(d)
		require.NoError(err)
		require.Equal(pinfos, result)
	}
}

func TestStoreGetOriginsResilientToUnavailableOrigins(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(Config{}, clock.New())

	d := core.DigestFixture()
	octxs, addrs, pinfos := originViews(3)

	dnsClient := mocks.expectClient(_testDNS)
	dnsClient.EXPECT().Locations(d).Return(addrs, nil)

	// Only one origin available.
	available := 1
	for i, octx := range octxs {
		client := mocks.expectClient(octx.IP)
		if i < available {
			client.EXPECT().GetPeerContext().Return(octx, nil)
		} else {
			client.EXPECT().GetPeerContext().Return(core.PeerContext{}, errors.New("some error"))
		}
	}

	// Ensure caching.
	for i := 0; i < 100; i++ {
		result, err := store.GetOrigins(d)
		require.NoError(err)
		require.Equal(pinfos[:available], result)
	}
}

func TestStoreGetOriginsErrorOnAllUnavailable(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(Config{}, clock.New())

	d := core.DigestFixture()
	octxs, addrs, _ := originViews(3)

	dnsClient := mocks.expectClient(_testDNS)
	dnsClient.EXPECT().Locations(d).Return(addrs, nil)

	for _, octx := range octxs {
		client := mocks.expectClient(octx.IP)
		client.EXPECT().GetPeerContext().Return(core.PeerContext{}, errors.New("some error"))
	}

	// Ensure caching.
	for i := 0; i < 100; i++ {
		_, err := store.GetOrigins(d)
		require.Error(err)
		_, ok := err.(allUnavailableError)
		require.True(ok)
	}
}

func TestStoreGetOriginsErrorTTL(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	clk := clock.NewMock()
	config := Config{
		LocationsTTL:         time.Minute,
		OriginUnavailableTTL: 30 * time.Second,
	}

	store := mocks.new(config, clk)

	d := core.DigestFixture()
	octxs, addrs, pinfos := originViews(3)

	dnsClient := mocks.expectClient(_testDNS)
	dnsClient.EXPECT().Locations(d).Return(addrs, nil)

	for _, octx := range octxs {
		client := mocks.expectClient(octx.IP)
		client.EXPECT().GetPeerContext().Return(core.PeerContext{}, errors.New("some error"))
	}

	for i := 0; i < 100; i++ {
		_, err := store.GetOrigins(d)
		require.Error(err)
		_, ok := err.(allUnavailableError)
		require.True(ok)
	}

	// Errors should be cleared now.
	clk.Add(config.OriginUnavailableTTL + 1)

	for _, octx := range octxs {
		client := mocks.expectClient(octx.IP)
		client.EXPECT().GetPeerContext().Return(octx, nil)
	}

	for i := 0; i < 100; i++ {
		result, err := store.GetOrigins(d)
		require.NoError(err)
		require.Equal(pinfos, result)
	}
}

func TestStoreGetOriginsCacheTTL(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	clk := clock.NewMock()
	config := Config{
		LocationsTTL:     time.Minute,
		OriginContextTTL: 30 * time.Second,
	}

	store := mocks.new(config, clk)

	d := core.DigestFixture()
	octxs, addrs, pinfos := originViews(3)

	dnsClient := mocks.expectClient(_testDNS)
	dnsClient.EXPECT().Locations(d).Return(addrs, nil)

	for _, octx := range octxs {
		client := mocks.expectClient(octx.IP)
		client.EXPECT().GetPeerContext().Return(octx, nil)
	}

	for i := 0; i < 100; i++ {
		result, err := store.GetOrigins(d)
		require.NoError(err)
		require.Equal(pinfos, result)
	}

	// Cached contexts should be cleared now.
	clk.Add(config.OriginContextTTL + 1)

	for _, octx := range octxs {
		client := mocks.expectClient(octx.IP)
		client.EXPECT().GetPeerContext().Return(core.PeerContext{}, errors.New("some error"))
	}

	for i := 0; i < 100; i++ {
		_, err := store.GetOrigins(d)
		require.Error(err)
		_, ok := err.(allUnavailableError)
		require.True(ok)
	}

}
