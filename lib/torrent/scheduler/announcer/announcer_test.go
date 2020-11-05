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
package announcer

import (
	"errors"
	"testing"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/mocks/tracker/announceclient"
	"github.com/uber/kraken/tracker/announceclient"
	"go.uber.org/zap"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

// How long to wait for the Ticker goroutine to fire / not fire. Fairly large
// to prevent flakey tests.
const _tickerTimeout = time.Second

type mockEvents struct {
	tick chan struct{}
}

func newMockEvents() *mockEvents {
	return &mockEvents{make(chan struct{}, 1)}
}

func (e *mockEvents) AnnounceTick() { e.tick <- struct{}{} }

func (e *mockEvents) expectTick(t *testing.T) {
	select {
	case <-e.tick:
	case <-time.After(_tickerTimeout):
		require.FailNow(t, "Tick timed out")
	}
}

func (e *mockEvents) expectNoTick(t *testing.T) {
	select {
	case <-e.tick:
		require.FailNow(t, "Unexpected tick")
	case <-time.After(_tickerTimeout):
	}
}

type announcerMocks struct {
	client *mockannounceclient.MockClient
	events *mockEvents
	clk    *clock.Mock
}

func newAnnouncerMocks(t *testing.T) (*announcerMocks, func()) {
	ctrl := gomock.NewController(t)
	return &announcerMocks{
		client: mockannounceclient.NewMockClient(ctrl),
		events: newMockEvents(),
		clk:    clock.NewMock(),
	}, ctrl.Finish
}

func (m *announcerMocks) newAnnouncer(config Config) *Announcer {
	return New(config, m.client, m.events, m.clk, zap.NewNop().Sugar())
}

func TestAnnouncerAnnounceUpdatesInterval(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAnnouncerMocks(t)
	defer cleanup()

	config := Config{DefaultInterval: 5 * time.Second}

	announcer := mocks.newAnnouncer(config)

	go announcer.Ticker(nil)

	mocks.clk.Add(config.DefaultInterval)
	mocks.events.expectTick(t)

	d := core.DigestFixture()
	hash := core.InfoHashFixture()
	interval := 10 * time.Second
	peers := []*core.PeerInfo{core.PeerInfoFixture()}

	mocks.client.EXPECT().Announce(d, hash, false, announceclient.V2).Return(peers, interval, nil)

	result, err := announcer.Announce(d, hash, false)
	require.NoError(err)
	require.Equal(peers, result)

	mocks.clk.Add(config.DefaultInterval)
	mocks.events.expectTick(t)

	// Timer should have been reset to new interval now.

	mocks.clk.Add(config.DefaultInterval)
	mocks.events.expectNoTick(t)

	mocks.clk.Add(interval - config.DefaultInterval)
	mocks.events.expectTick(t)
}

func TestAnnouncerAnnounceErr(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAnnouncerMocks(t)
	defer cleanup()

	announcer := mocks.newAnnouncer(Config{})

	go announcer.Ticker(nil)

	d := core.DigestFixture()
	hash := core.InfoHashFixture()
	err := errors.New("some error")

	mocks.client.EXPECT().Announce(d, hash, false, announceclient.V2).Return(nil, time.Duration(0), err)

	_, aErr := announcer.Announce(d, hash, false)
	require.Equal(err, aErr)
}
