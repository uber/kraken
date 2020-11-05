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
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/tracker/announceclient"

	"github.com/andres-erbsen/clock"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Config defines Announcer configuration.
type Config struct {
	DefaultInterval time.Duration `yaml:"default_interval"`
	MaxInterval     time.Duration `yaml:"max_interval"`
}

func (c Config) applyDefaults() Config {
	if c.DefaultInterval == 0 {
		c.DefaultInterval = 5 * time.Second
	}
	if c.MaxInterval == 0 {
		c.MaxInterval = time.Minute
	}
	return c
}

// Events defines Announcer events.
type Events interface {
	AnnounceTick()
}

// Announcer is a thin wrapper around an announceclient.Client which handles
// changes to the announce interval.
type Announcer struct {
	config   Config
	client   announceclient.Client
	events   Events
	interval *atomic.Int64
	timer    *clock.Timer
	logger   *zap.SugaredLogger
}

// New creates a new Announcer.
func New(
	config Config,
	client announceclient.Client,
	events Events,
	clk clock.Clock,
	logger *zap.SugaredLogger) *Announcer {
	config = config.applyDefaults()
	return &Announcer{
		config:   config,
		client:   client,
		events:   events,
		interval: atomic.NewInt64(int64(config.DefaultInterval)),
		timer:    clk.Timer(config.DefaultInterval),
		logger:   logger,
	}
}

// Default creates a default Announcer.
// TODO(evelynl94): make announce interval configurable.
func Default(
	client announceclient.Client,
	events Events,
	clk clock.Clock,
	logger *zap.SugaredLogger) *Announcer {
	return New(Config{}, client, events, clk, logger)
}

// Announce announces through the underlying client and returns the resulting
// peer handout. Updates the announce interval if it has changed.
func (a *Announcer) Announce(
	d core.Digest, h core.InfoHash, complete bool) ([]*core.PeerInfo, error) {

	peers, interval, err := a.client.Announce(d, h, complete, announceclient.V2)
	if err != nil {
		return nil, err
	}
	if interval == 0 {
		// Protect against unset intervals.
		interval = a.config.DefaultInterval
	}
	if interval > a.config.MaxInterval {
		// Since the timer is only reset on ticks, a wildly high interval can lock
		// down future updates to interval. The max interval protects against a
		// mistake in the central authority which will become impossible to correct.
		interval = a.config.DefaultInterval
	}
	if a.interval.Swap(int64(interval)) != int64(interval) {
		// Note: updated interval will take effect after next tick.
		a.logger.Infof("Announce interval updated to %s", interval)
	}
	return peers, nil
}

// Ticker emits AnnounceTick events at the current announce interval, which may be
// updated by Announce. Ticker exits when done is closed.
func (a *Announcer) Ticker(done <-chan struct{}) {
	for {
		select {
		case <-a.timer.C:
			a.events.AnnounceTick()
			a.timer.Reset(time.Duration(a.interval.Load()))
		case <-done:
			return
		}
	}
}
