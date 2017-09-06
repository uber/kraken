package scheduler

import (
	"errors"
	"time"

	"code.uber.internal/infra/kraken/utils/memsize"

	"github.com/andres-erbsen/clock"
)

// Config is the Scheduler configuration.
type Config struct {

	// ListenAddr is the "ip:port" the Scheduler will serve connections on.
	ListenAddr string `yaml:"listen_addr"`

	// Datacenter is the current datacenter.
	Datacenter string `yaml:"datacenter"`

	// TrackerAddr is the "ip:port" of the tracker server.
	TrackerAddr string `yaml:"tracker_addr"`

	// AnnounceInterval is the time between all announce requests.
	// TODO(codyg): Make this smarter -- ideally, we give priority on announcing based on the
	// following criteria:
	// 1. Torrents which are making little progress
	// 2. Higher priority torrents
	AnnounceInterval time.Duration `yaml:"announce_interval"`

	// DialTimeout is the timeout for opening new connections.
	DialTimeout time.Duration `yaml:"dial_timeout"`

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

	// Clock allows overriding the Scheduler clock. Defaults to system clock.
	Clock clock.Clock

	ConnState ConnStateConfig `yaml:"conn_state"`

	Conn ConnConfig `yaml:"conn"`
}

func (c Config) applyDefaults() (Config, error) {
	if c.TrackerAddr == "" {
		return c, errors.New("no tracker addr specified")
	}
	if c.ListenAddr == "" {
		return c, errors.New("no listen addr specified")
	}
	if c.Datacenter == "" {
		return c, errors.New("no datacenter specified")
	}
	if c.AnnounceInterval == 0 {
		c.AnnounceInterval = 30 * time.Second
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.IdleSeederTTL == 0 {
		c.IdleSeederTTL = 10 * time.Minute
	}
	if c.PreemptionInterval == 0 {
		c.PreemptionInterval = 30 * time.Second
	}
	if c.IdleConnTTL == 0 {
		c.IdleConnTTL = 5 * time.Minute
	}
	if c.ConnTTL == 0 {
		c.ConnTTL = time.Hour
	}
	if c.BlacklistCleanupInterval == 0 {
		c.BlacklistCleanupInterval = 10 * time.Minute
	}
	if c.Clock == nil {
		c.Clock = clock.New()
	}
	c.ConnState = c.ConnState.applyDefaults()
	c.Conn = c.Conn.applyDefaults()
	return c, nil
}

// ConnStateConfig is the configuration for the connection state management.
type ConnStateConfig struct {

	// MaxOpenConnectionsPerTorrent is the maximum number of connections which a
	// Scheduler will maintain at once for each torrent.
	MaxOpenConnectionsPerTorrent int `yaml:"max_open_conn"`

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
		c.MaxOpenConnectionsPerTorrent = 20
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

// ConnConfig is the configuration for individual live connections.
type ConnConfig struct {

	// WriteTimeout is the timeout for connection writes.
	WriteTimeout time.Duration `yaml:"write_timeout"`

	// SenderBufferSize is the size of the sender channel for a connection.
	// Prevents writers to the connection from being blocked if there are many
	// writers trying to send messages at the same time.
	SenderBufferSize int `yaml:"sender_buffer_size"`

	// ReceiverBufferSize is the size of the receiver channel for a connection.
	// Prevents the connection reader from being blocked if a receiver consumer
	// is taking a long time to process a message.
	ReceiverBufferSize int `yaml:"reciver_buffer_size"`
}

func (c ConnConfig) applyDefaults() ConnConfig {
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 5 * time.Second
	}
	// TODO(codyg): We cannot set default buffer sizes, since 0 is a very valid
	// buffer size for testing.
	return c
}
