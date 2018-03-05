package scheduler

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v2"

	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
)

// Config is the Scheduler configuration.
type Config struct {

	// AnnounceInterval is the time between all announce requests.
	// TODO(codyg): Make this smarter -- ideally, we give priority on announcing based on the
	// following criteria:
	// 1. Torrents which are making little progress
	// 2. Higher priority torrents
	AnnounceInterval time.Duration `yaml:"announce_interval"`

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

	ConnState ConnStateConfig `yaml:"conn_state"`

	Conn conn.Config `yaml:"conn"`

	Dispatcher DispatcherConfig `yaml:"dispatcher"`
}

func (c Config) String() string {
	b, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Sprintf("yaml marshal error: %s", err)
	}
	return string(b)
}

func (c Config) applyDefaults() Config {
	if c.AnnounceInterval == 0 {
		c.AnnounceInterval = 5 * time.Second
	}
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
	c.ConnState = c.ConnState.applyDefaults()
	c.Dispatcher = c.Dispatcher.applyDefaults()
	return c
}

// ConnStateConfig is the configuration for the connection state management.
type ConnStateConfig struct {

	// MaxOpenConnectionsPerTorrent is the maximum number of connections which a
	// Scheduler will maintain at once for each torrent.
	MaxOpenConnectionsPerTorrent int `yaml:"max_open_conn"`

	// DisableBlacklist disables the blacklisting of peers. Should only be used
	// for testing purposes.
	DisableBlacklist bool `yaml:"disable_blacklist"`

	// BlacklistDuration is the duration a connection will remain blacklisted.
	BlacklistDuration time.Duration `yaml:"blacklist_duration"`
}

func (c ConnStateConfig) applyDefaults() ConnStateConfig {
	if c.MaxOpenConnectionsPerTorrent == 0 {
		c.MaxOpenConnectionsPerTorrent = 10
	}
	if c.BlacklistDuration == 0 {
		c.BlacklistDuration = 30 * time.Second
	}
	return c
}

// DispatcherConfig is the configuration for piece dispatch.
type DispatcherConfig struct {

	// PieceRequestMinTimeout is the minimum timeout for all piece requests, regardless of
	// size.
	PieceRequestMinTimeout time.Duration `yaml:"piece_request_min_timeout"`

	// PieceRequestTimeoutPerMb is the duration that will be added to piece request
	// timeouts based on the piece size (in megabytes).
	PieceRequestTimeoutPerMb time.Duration `yaml:"piece_request_timeout_per_mb"`

	// PipelineLimit limits the total number of requests can be sent to a peer
	// at the same time.
	PipelineLimit int `yaml:"pipeline_limit"`

	// EndgameThreshold is the number pieces required to complete the torrent
	// before the torrent enters "endgame", where we start overloading piece
	// requests to multiple peers.
	EndgameThreshold int `yaml:"endgame_threshold"`

	DisableEndgame bool `yaml:"disable_endgame"`
}

func (c DispatcherConfig) applyDefaults() DispatcherConfig {
	if c.PieceRequestMinTimeout == 0 {
		c.PieceRequestMinTimeout = 4 * time.Second
	}
	if c.PieceRequestTimeoutPerMb == 0 {
		c.PieceRequestTimeoutPerMb = 4 * time.Second
	}
	if c.PipelineLimit == 0 {
		c.PipelineLimit = 3
	}
	if c.EndgameThreshold == 0 {
		c.EndgameThreshold = c.PipelineLimit
	}
	return c
}
