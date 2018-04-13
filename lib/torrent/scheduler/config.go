package scheduler

import (
	"time"

	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/connstate"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/dispatch"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/torrentlog"
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

	TorrentLog torrentlog.Config `yaml:"torrentlog"`
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
