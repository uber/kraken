package torrent

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
)

const requestTimeout = 60 * time.Second
const downloadTimeout = 10 * time.Minute

// Client TODO
type Client interface {
	Download(name string) (store.FileReader, error)
	Reload(config scheduler.Config)
	Close() error
}

// SchedulerClient is a client for scheduler
type SchedulerClient struct {
	config Config

	mu        sync.Mutex // Protects reloading scheduler.
	scheduler *scheduler.Scheduler

	stats tally.Scope
	fs    store.FileStore
}

// NewSchedulerClient creates a new scheduler client
func NewSchedulerClient(
	config Config,
	fs store.FileStore,
	stats tally.Scope,
	pctx peercontext.PeerContext,
	announceClient announceclient.Client,
	metaInfoClient metainfoclient.Client) (Client, error) {

	stats = stats.SubScope("peer").SubScope(pctx.PeerID.String())
	archive := storage.NewLocalTorrentArchive(fs, metaInfoClient)
	networkEventProducer, err := networkevent.NewProducer(config.NetworkEvent)
	if err != nil {
		return nil, fmt.Errorf("network event producer: %s", err)
	}
	sched, err := scheduler.New(
		config.Scheduler, archive, stats, pctx, announceClient, networkEventProducer)
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
func (c *SchedulerClient) Download(name string) (store.FileReader, error) {
	if !c.config.Enabled {
		return nil, errors.New("torrent not enabled")
	}

	stopwatch := c.stats.Timer("download_torrent_time").Start()
	select {
	case err := <-c.scheduler.AddTorrent(name):
		if err != nil {
			c.stats.Counter("download_torrent_errors").Inc(1)
			return nil, fmt.Errorf("failed to schedule torrent: %s", err)
		}
		stopwatch.Stop()
	case <-time.After(downloadTimeout):
		c.stats.Counter("download_torrent_timeouts").Inc(1)
		c.scheduler.CancelTorrent(name)
		return nil, fmt.Errorf("scheduled torrent timed out after %.2f seconds", downloadTimeout.Seconds())
	}

	f, err := c.fs.GetCacheFileReader(name)
	if err != nil {
		return nil, fmt.Errorf("get cache file reader: %s", err)
	}
	return f, nil
}
