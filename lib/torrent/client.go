package torrent

import (
	"errors"
	"fmt"
	"io"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer/manifestclient"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
)

const requestTimeout = 60 * time.Second
const downloadTimeout = 120 * time.Second

// Client TODO
type Client interface {
	DownloadTorrent(name string) (io.ReadCloser, error)
	GetManifest(repo, tag string) (io.ReadCloser, error)
	PostManifest(repo, tag, digest string, manifest io.Reader) error
	Close() error
}

// SchedulerClient is a client for scheduler
type SchedulerClient struct {
	config    *Config
	peerID    torlib.PeerID
	scheduler *scheduler.Scheduler
	stats     tally.Scope

	// TODO: Consolidate these...
	store   store.FileStore
	archive storage.TorrentArchive

	manifestClient manifestclient.Client
}

// NewSchedulerClient creates a new scheduler client
func NewSchedulerClient(
	config *Config,
	localStore store.FileStore,
	stats tally.Scope,
	pctx peercontext.PeerContext,
	announceClient announceclient.Client,
	manifestClient manifestclient.Client,
	metaInfoClient metainfoclient.Client) (Client, error) {

	stats = stats.SubScope("peer").SubScope(pctx.PeerID.String())
	archive := storage.NewLocalTorrentArchive(localStore, metaInfoClient)
	networkEventProducer, err := networkevent.NewProducer(config.NetworkEvent)
	if err != nil {
		return nil, fmt.Errorf("network event producer: %s", err)
	}
	scheduler, err := scheduler.New(
		config.Scheduler, archive, stats, pctx, announceClient, networkEventProducer)
	if err != nil {
		return nil, fmt.Errorf("scheduler: %s", err)
	}
	return &SchedulerClient{
		config:         config,
		peerID:         pctx.PeerID,
		scheduler:      scheduler,
		stats:          stats,
		store:          localStore,
		archive:        archive,
		manifestClient: manifestClient,
	}, nil
}

// Close stops scheduler
func (c *SchedulerClient) Close() error {
	c.scheduler.Stop()
	return nil
}

// DownloadTorrent downloads a torrent given torrent name
func (c *SchedulerClient) DownloadTorrent(name string) (io.ReadCloser, error) {
	stopwatch := c.stats.SubScope("torrent").SubScope(name).Timer("download_time").Start()

	if !c.config.Enabled {
		return nil, errors.New("torrent not enabled")
	}

	select {
	case err := <-c.scheduler.AddTorrent(name):
		if err != nil {
			return nil, fmt.Errorf("failed to schedule torrent: %s", err)
		}
	case <-time.After(downloadTimeout):
		c.scheduler.CancelTorrent(name)
		return nil, fmt.Errorf("scheduled torrent timed out after %.2f seconds", downloadTimeout.Seconds())
	}

	stopwatch.Stop()
	return c.store.GetCacheFileReader(name)
}

// DropTorrent TODO
func (c *SchedulerClient) DropTorrent(infoHash torlib.InfoHash) error {
	// TODO
	return nil
}

// GetManifest queries tracker for manifest and stores manifest locally
func (c *SchedulerClient) GetManifest(repo, tag string) (io.ReadCloser, error) {
	if !c.config.Enabled {
		return nil, errors.New("torrent not enabled")
	}
	return c.manifestClient.GetManifest(repo, tag)
}

// PostManifest saves manifest specified by the tag it referred in a tracker
func (c *SchedulerClient) PostManifest(repo, tag, digest string, manifest io.Reader) error {
	if !c.config.Enabled {
		log.Info("Skipping post manifest: torrent not enabled")
		return nil
	}
	return c.manifestClient.PostManifest(repo, tag, digest, manifest)
}
