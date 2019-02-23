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
package scheduler

import (
	"time"

	"github.com/uber/kraken/lib/torrent/scheduler/conn"
	"github.com/uber/kraken/lib/torrent/scheduler/connstate"
	"github.com/uber/kraken/lib/torrent/scheduler/dispatch"
	"github.com/uber/kraken/utils/log"
)

// Config is the Scheduler configuration.
type Config struct {

	// SeederTTI is the duration a seeding torrent will exist without being
	// read from before being cancelled.
	SeederTTI time.Duration `yaml:"seeder_tti"`

	// LeecherTTI is the duration a leeching torrent will exist without being
	// written to before being cancelled.
	LeecherTTI time.Duration `yaml:"leecher_tti"`

	// ConnTTI is the duration a connection will exist without transmitting any
	// needed pieces or requesting any pieces.
	ConnTTI time.Duration `yaml:"conn_tti"`

	// ConnTTL is the max duration a connection may exist regardless of liveness.
	ConnTTL time.Duration `yaml:"conn_ttl"`

	// PreemptionInterval is the interval in which the Scheduler analyzes the
	// status of existing conns and determines whether to preempt them.
	PreemptionInterval time.Duration `yaml:"preemption_interval"`

	// EmitStatsInterval is the interval introspective stats are emitted from
	// the Scheduler.
	EmitStatsInterval time.Duration `yaml:"emit_stats_interval"`

	// DisablePreemption disables resource preemption. Should only be used for
	// testing purposes.
	DisablePreemption bool `yaml:"disable_preemption"`

	ProbeTimeout time.Duration `yaml:"probe_timeout"`

	ConnState connstate.Config `yaml:"connstate"`

	Conn conn.Config `yaml:"conn"`

	Dispatch dispatch.Config `yaml:"dispatch"`

	TorrentLog log.Config `yaml:"torrentlog"`
	Log        log.Config `yaml:"log"`
}

func (c Config) applyDefaults() Config {
	if c.SeederTTI == 0 {
		c.SeederTTI = 5 * time.Minute
	}
	if c.LeecherTTI == 0 {
		c.LeecherTTI = 5 * time.Minute
	}
	if c.ConnTTI == 0 {
		c.ConnTTI = 30 * time.Second
	}
	if c.ConnTTL == 0 {
		c.ConnTTL = time.Hour
	}
	if c.PreemptionInterval == 0 {
		c.PreemptionInterval = 30 * time.Second
	}
	if c.EmitStatsInterval == 0 {
		c.EmitStatsInterval = 1 * time.Second
	}
	if c.ProbeTimeout == 0 {
		c.ProbeTimeout = 3 * time.Second
	}
	return c
}
