package scheduler

import "time"

// Config is the Scheduler configuration.
type Config struct {

	// TrackerAddr is the "ip:port" of the tracker server.
	TrackerAddr string

	// MaxOpenConnectionsPerTorrent is the maximum number of connections which a
	// Scheduler will maintain at once for each torrent.
	MaxOpenConnectionsPerTorrent int

	// AnnounceInterval is the time between all announce requests.
	// TODO(codyg): Make this smarter -- ideally, we give priority on announcing based on the
	// following criteria:
	// 1. Torrents which are making little progress
	// 2. Higher priority torrents
	AnnounceInterval time.Duration

	// DialTimeout is the timeout for opening new connections.
	DialTimeout time.Duration

	// WriteTimeout is the timeout for connection writes.
	WriteTimeout time.Duration

	// SenderBufferSize is the size of the sender channel for a connection.
	// Prevents writers to the connection from being blocked if there are many
	// writers trying to send messages at the same time.
	SenderBufferSize int

	// ReceiverBufferSize is the size of the receiver channel for a connection.
	// Prevents the connection reader from being blocked if a receiver consumer
	// is taking a long time to process a message.
	ReceiverBufferSize int

	// IdleSeederTTL is the duration an idle dispatcher will exist after
	// completing its torrent.
	IdleSeederTTL time.Duration

	// PreemptionInterval is the interval in which the Scheduler analyzes the
	// status of existing conns and determines whether to preempt them.
	PreemptionInterval time.Duration

	// IdleConnTTL is the duration an idle connection will exist before
	// being closed. An idle connection is defined as a connection which is not
	// transmitting any needed pieces or requesting any pieces.
	IdleConnTTL time.Duration

	// ConnTTL is the max duration a connection may exist regardless of liveness.
	ConnTTL time.Duration

	// InitialBlacklistExpiration is how long a connection will be blacklisted
	// after its first close.
	InitialBlacklistExpiration time.Duration

	// BlacklistExpirationBackoff is the power at which the blacklist expiration
	// time exponentially rises for repeatedly blacklisted connections. Must be
	// greater than or equal to 1.
	BlacklistExpirationBackoff float64

	// MaxBlacklistExpiration is the max duration the blacklist expiration backoff
	// will rise to.
	MaxBlacklistExpiration time.Duration

	// ExpiredBlacklistEntryTTL is the duration an expired blacklist entry will
	// exist before being deleted, essentially resetting its backoff. This is
	// only necessary for ensuring unused memory is eventually reclaimed.
	ExpiredBlacklistEntryTTL time.Duration

	// BlacklistCleanupInterval is the interval expired blacklist entries which
	// have surpassed their TTL are removed.
	BlacklistCleanupInterval time.Duration
}
