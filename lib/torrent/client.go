package torrent

import (
	"fmt"
	"log"
	"sync"

	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/torrent/announcequeue"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/connstate"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/tracker/announceclient"
)

// Client TODO
type Client interface {
	Download(namespace string, name string) error
	Reload(config scheduler.Config)
	BlacklistSnapshot() ([]connstate.BlacklistedConn, error)
	RemoveTorrent(name string) error
	Probe() error
	Close() error
}

// SchedulerClient is a client for scheduler
type SchedulerClient struct {
	config Config

	mu        sync.Mutex // Protects reloading scheduler.
	scheduler *scheduler.Scheduler

	stats tally.Scope
}

// NewSchedulerClient creates a new scheduler client
func NewSchedulerClient(
	config Config,
	stats tally.Scope,
	pctx core.PeerContext,
	announceClient announceclient.Client,
	announceQueue announcequeue.Queue,
	archive storage.TorrentArchive) (Client, error) {

	networkEvents, err := networkevent.NewProducer(config.NetworkEvent)
	if err != nil {
		return nil, fmt.Errorf("network event producer: %s", err)
	}

	sched, err := scheduler.New(
		config.Scheduler, archive, stats, pctx, announceClient, announceQueue, networkEvents)
	if err != nil {
		return nil, fmt.Errorf("scheduler: %s", err)
	}

	stats = stats.Tagged(map[string]string{
		"module": "torrentclient",
	})

	return &SchedulerClient{
		config:    config,
		scheduler: sched,
		stats:     stats,
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
	err := <-c.scheduler.AddTorrent(namespace, name)
	if err != nil {
		var tag string
		switch err {
		case scheduler.ErrTorrentNotFound:
			tag = "not_found"
		case scheduler.ErrTorrentTimeout:
			tag = "timeout"
		case scheduler.ErrSchedulerStopped:
			tag = "scheduler_stopped"
		case scheduler.ErrTorrentRemoved:
			tag = "removed"
		default:
			tag = "unknown"
		}
		c.stats.Tagged(map[string]string{
			"error": tag,
		}).Counter("download_torrent_errors").Inc(1)
		return err
	}
	return nil
}

// BlacklistSnapshot returns the currently blacklisted connections for this peer.
func (c *SchedulerClient) BlacklistSnapshot() ([]connstate.BlacklistedConn, error) {
	result, err := c.scheduler.BlacklistSnapshot()
	if err != nil {
		return nil, err
	}
	return <-result, nil
}

// RemoveTorrent forcibly stops torrent for name, preventing it from downloading / seeding
// any further.
func (c *SchedulerClient) RemoveTorrent(name string) error {
	return <-c.scheduler.RemoveTorrent(name)
}

// Probe verifies that the Scheduler event loop is running and unblocked.
func (c *SchedulerClient) Probe() error {
	return c.scheduler.Probe()
}
