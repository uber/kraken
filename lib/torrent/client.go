package torrent

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/torrent/announcequeue"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/tracker/announceclient"
)

// Client TODO
type Client interface {
	Download(namespace string, name string) error
	Reload(config scheduler.Config)
	BlacklistSnapshot() ([]scheduler.BlacklistedConn, error)
	Close() error
}

// SchedulerClient is a client for scheduler
type SchedulerClient struct {
	config Config

	mu        sync.Mutex // Protects reloading scheduler.
	scheduler *scheduler.Scheduler

	stats     tally.Scope
	hostStats tally.Scope
}

// NewSchedulerClient creates a new scheduler client
func NewSchedulerClient(
	config Config,
	stats tally.Scope,
	pctx peercontext.PeerContext,
	announceClient announceclient.Client,
	announceQueue announcequeue.Queue,
	archive storage.TorrentArchive) (Client, error) {

	// NOTE: M3 will drop metrics that contain 32 consecutive hexadecimal characters,
	// so we cannot emit full peer ids. Instead, we emit a combination of hostname
	// (which will almost always have a 1-1 mapping with peer id) and a shortened
	// peer id to catch cases where there may be multiple peers on the same host.
	shortenedPID := pctx.PeerID.String()[:8]
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("hostname: %s", err)
	}
	hostStats := stats.Tagged(map[string]string{
		"peer":     shortenedPID,
		"hostname": hostname,
	})

	networkEvents, err := networkevent.NewProducer(config.NetworkEvent)
	if err != nil {
		return nil, fmt.Errorf("network event producer: %s", err)
	}

	sched, err := scheduler.New(
		config.Scheduler, archive, stats, pctx, announceClient, announceQueue, networkEvents)
	if err != nil {
		return nil, fmt.Errorf("scheduler: %s", err)
	}

	return &SchedulerClient{
		config:    config,
		scheduler: sched,
		stats:     stats,
		hostStats: hostStats,
	}, nil
}

// Reload restarts the client with new configuration.
func (c *SchedulerClient) Reload(config scheduler.Config) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s, err := scheduler.Reload(c.scheduler, config, c.stats)
	if err != nil {
		// Totally unrecoverable error -- c.scheduler is now stopped and unusable,
		// so let process die and restart with original config.
		log.Fatalf("Failed to reload scheduler config: %s", err)
	}
	c.scheduler = s
}

// Close stops scheduler
func (c *SchedulerClient) Close() error {
	c.scheduler.Stop()
	return nil
}

// Download downloads blob identified by name into the file store cache.
// Returns scheduler.ErrTorrentNotFound if no torrent for namespace / name was
// found.
func (c *SchedulerClient) Download(namespace string, name string) error {
	if err := <-c.scheduler.AddTorrent(namespace, name); err != nil {
		c.hostStats.Counter("download_torrent_errors").Inc(1)
		return err
	}
	return nil
}

// BlacklistSnapshot returns the currently blacklisted connections for this peer.
func (c *SchedulerClient) BlacklistSnapshot() ([]scheduler.BlacklistedConn, error) {
	result, err := c.scheduler.BlacklistSnapshot()
	if err != nil {
		return nil, err
	}
	return <-result, nil
}
