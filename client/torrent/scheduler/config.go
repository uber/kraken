package scheduler

import (
	"time"

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

	// MaxOpenConnectionsPerTorrent is the maximum number of connections which a
	// Scheduler will maintain at once for each torrent.
	MaxOpenConnectionsPerTorrent int `yaml:"max_open_conn"`

	// AnnounceInterval is the time between all announce requests.
	// TODO(codyg): Make this smarter -- ideally, we give priority on announcing based on the
	// following criteria:
	// 1. Torrents which are making little progress
	// 2. Higher priority torrents
	AnnounceInterval time.Duration `yaml:"announce_interval"`

	// DialTimeout is the timeout for opening new connections.
	DialTimeout time.Duration `yaml:"dial_timeout"`

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

	// BlacklistCleanupInterval is the interval expired blacklist entries which
	// have surpassed their TTL are removed.
	BlacklistCleanupInterval time.Duration `yaml:"blacklist_cleanup_interval"`

	// Clock allows overriding the Scheduler clock. Defaults to system clock.
	Clock clock.Clock
}
