package announcer

import (
	"errors"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/mocks/tracker/announceclient"
	"code.uber.internal/infra/kraken/tracker/announceclient"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

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
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "Tick timed out")
	}
}

func (e *mockEvents) expectNoTick(t *testing.T) {
	select {
	case <-e.tick:
		require.FailNow(t, "Unexpected tick")
	case <-time.After(250 * time.Millisecond):
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
	return New(config, m.client, m.events, m.clk)
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

	name := core.DigestFixture().Hex()
	hash := core.InfoHashFixture()
	interval := 10 * time.Second
	peers := []*core.PeerInfo{core.PeerInfoFixture()}

	mocks.client.EXPECT().Announce(name, hash, false, announceclient.V1).Return(peers, interval, nil)

	result, err := announcer.Announce(name, hash, false)
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

	name := core.DigestFixture().Hex()
	hash := core.InfoHashFixture()
	err := errors.New("some error")

	mocks.client.EXPECT().Announce(name, hash, false, announceclient.V1).Return(nil, time.Duration(0), err)

	_, aErr := announcer.Announce(name, hash, false)
	require.Equal(err, aErr)
}
