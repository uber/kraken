package torrent

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/tracker/announceclient"
)

const requestTimeout = 60 * time.Second
const downloadTimeout = 10 * time.Minute

// Client TODO
type Client interface {
	Download(namespace string, name string) (store.FileReader, error)
	Reload(config scheduler.Config)
	BlacklistSnapshot() ([]scheduler.BlacklistedConn, error)
	Close() error
}

// SchedulerClient is a client for scheduler
type SchedulerClient struct {
	config Config

	mu        sync.Mutex // Protects reloading scheduler.
	scheduler *scheduler.Scheduler

	stats tally.Scope
	fs    store.ReadOnlyFileStore
}

// NewSchedulerClient creates a new scheduler client
func NewSchedulerClient(
	config Config,
	fs store.ReadOnlyFileStore,
	stats tally.Scope,
	pctx peercontext.PeerContext,
	announceClient announceclient.Client,
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
	stats = stats.SubScope("peer").SubScope(hostname).SubScope(shortenedPID)

	networkEvents, err := networkevent.NewProducer(config.NetworkEvent)
	if err != nil {
		return nil, fmt.Errorf("network event producer: %s", err)
	}

	sched, err := scheduler.New(
		config.Scheduler, archive, stats, pctx, announceClient, networkEvents)
	if err != nil {
		return nil, fmt.Errorf("scheduler: %s", err)
	}

	return &SchedulerClient{
		config:    config,
		scheduler: sched,
		stats:     stats,
		fs:        fs,
	}, nil
}

// Reload restarts the client with new configuration.
func (c *SchedulerClient) Reload(config scheduler.Config) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s, err := scheduler.Reload(c.scheduler, config)
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
func (c *SchedulerClient) Download(namespace string, name string) (store.FileReader, error) {
	stopwatch := c.stats.Timer("download_torrent_time").Start()
	if err := <-c.scheduler.AddTorrent(namespace, name); err != nil {
		c.stats.Counter("download_torrent_errors").Inc(1)
		return nil, err
	}
	stopwatch.Stop()
	f, err := c.fs.GetCacheFileReader(name)
	if err != nil {
		return nil, fmt.Errorf("get cache file reader: %s", err)
	}
	return f, nil
}

// BlacklistSnapshot returns the currently blacklisted connections for this peer.
func (c *SchedulerClient) BlacklistSnapshot() ([]scheduler.BlacklistedConn, error) {
	result, err := c.scheduler.BlacklistSnapshot()
	if err != nil {
		return nil, err
	}
	return <-result, nil
}
