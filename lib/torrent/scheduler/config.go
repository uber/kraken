package scheduler

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v2"

	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
	"code.uber.internal/infra/kraken/utils/memsize"
)

// Config is the Scheduler configuration.
type Config struct {

	// AnnounceInterval is the time between all announce requests.
	// TODO(codyg): Make this smarter -- ideally, we give priority on announcing based on the
	// following criteria:
	// 1. Torrents which are making little progress
	// 2. Higher priority torrents
	AnnounceInterval time.Duration `yaml:"announce_interval"`

	// IdleSeederTTL is the duration an idle dispatcher will exist after
	// completing its torrent.
	IdleSeederTTL time.Duration `yaml:"idle_seeder_ttl"`

	// PreemptionInterval is the interval in which the Scheduler analyzes the
	// status of existing conns and determines whether to preempt them.
	PreemptionInterval time.Duration `yaml:"preemption_interval"`

	// IdleConnTTL is the duration an idle connection will exist before
	// being closed. An idle connection is defined as a connection which is not
	// transmitting any needed pieces or requesting any pieces.
	IdleConnTTL time.Duration `yaml:"idle_conn_ttl"`

	// ConnTTL is the max duration a connection may exist regardless of liveness.
	ConnTTL time.Duration `yaml:"conn_ttl"`

	// BlacklistCleanupInterval is the interval expired blacklist entries which
	// have surpassed their TTL are removed.
	BlacklistCleanupInterval time.Duration `yaml:"blacklist_cleanup_interval"`

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
		c.AnnounceInterval = 3 * time.Second
	}
	if c.IdleSeederTTL == 0 {
		c.IdleSeederTTL = 10 * time.Minute
	}
	if c.PreemptionInterval == 0 {
		c.PreemptionInterval = 30 * time.Second
	}
	if c.IdleConnTTL == 0 {
		c.IdleConnTTL = 30 * time.Second
	}
	if c.ConnTTL == 0 {
		c.ConnTTL = time.Hour
	}
	if c.BlacklistCleanupInterval == 0 {
		c.BlacklistCleanupInterval = 10 * time.Minute
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

	// InitialBlacklistExpiration is how long a connection will be blacklisted
	// after its first close.
	InitialBlacklistExpiration time.Duration `yaml:"init_blacklist_expire"`

	// BlacklistExpirationBackoff is the power at which the blacklist expiration
	// time exponentially rises for repeatedly blacklisted connections. Must be
	// greater than or equal to 1.
	BlacklistExpirationBackoff float64 `yaml:"blacklist_expire_backoff"`

	// MaxBlacklistExpiration is the max duration the blacklist expiration backoff
	// will rise to.
	MaxBlacklistExpiration time.Duration `yaml:"max_blacklist_expiration"`

	// ExpiredBlacklistEntryTTL is the duration an expired blacklist entry will
	// exist before being deleted, essentially resetting its backoff. This is
	// only necessary for ensuring unused memory is eventually reclaimed.
	ExpiredBlacklistEntryTTL time.Duration `yaml:"expire_blacklist_ttl"`

	// MaxGlobalEgressBytesPerSec is the max number of piece payload bytes that
	// can be uploaded across all connections per second.
	MaxGlobalEgressBytesPerSec uint64 `yaml:"max_global_egress_bytes_per_sec"`

	// MinConnEgressBytesPerSec is the lowest bytes per second a connection's
	// egress piece payloads may be throttled to.
	MinConnEgressBytesPerSec uint64 `yaml:"min_conn_egress_bytes_per_sec"`
}

func (c ConnStateConfig) applyDefaults() ConnStateConfig {
	if c.MaxOpenConnectionsPerTorrent == 0 {
		c.MaxOpenConnectionsPerTorrent = 10
	}
	if c.InitialBlacklistExpiration == 0 {
		c.InitialBlacklistExpiration = time.Minute
	}
	if c.BlacklistExpirationBackoff == 0 {
		c.BlacklistExpirationBackoff = 2
	}
	if c.MaxBlacklistExpiration == 0 {
		c.MaxBlacklistExpiration = 30 * time.Minute
	}
	if c.ExpiredBlacklistEntryTTL == 0 {
		c.ExpiredBlacklistEntryTTL = 6 * time.Hour
	}
	if c.MaxGlobalEgressBytesPerSec == 0 {
		c.MaxGlobalEgressBytesPerSec = 5 * memsize.GB
	}
	if c.MinConnEgressBytesPerSec == 0 {
		c.MinConnEgressBytesPerSec = 2 * memsize.MB
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
}

func (c DispatcherConfig) applyDefaults() DispatcherConfig {
	if c.PieceRequestMinTimeout == 0 {
		c.PieceRequestMinTimeout = 4 * time.Second
	}
	if c.PieceRequestTimeoutPerMb == 0 {
		c.PieceRequestTimeoutPerMb = 4 * time.Second
	}
	return c
}
