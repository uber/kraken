package torrent

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer/manifestclient"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/store"
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
	CreateTorrentFromFile(name, filepath string) error
	GetPeerContext() (peercontext.PeerContext, error)
	GetManifest(repo, tag string) (io.ReadCloser, error)
	PostManifest(repo, tag, digest string, manifest io.Reader) error
	Close() error
}

// SchedulerClient is a client for scheduler
type SchedulerClient struct {
	config    *Config
	pctx      peercontext.PeerContext
	peerID    torlib.PeerID
	scheduler *scheduler.Scheduler
	stats     tally.Scope

	// TODO: Consolidate these...
	store   store.FileStore
	archive storage.TorrentArchive

	manifestClient manifestclient.Client
	metaInfoClient metainfoclient.Client
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
	archive := storage.NewLocalTorrentArchive(localStore)
	scheduler, err := scheduler.New(config.Scheduler, archive, stats, pctx, announceClient)
	if err != nil {
		return nil, err
	}
	return &SchedulerClient{
		config:         config,
		pctx:           pctx,
		peerID:         pctx.PeerID,
		scheduler:      scheduler,
		stats:          stats,
		store:          localStore,
		archive:        archive,
		manifestClient: manifestClient,
		metaInfoClient: metaInfoClient,
	}, nil
}

// Close stops scheduler
func (c *SchedulerClient) Close() error {
	c.scheduler.Stop()
	return nil
}

// DownloadTorrent downloads a torrent given torrent name
func (c *SchedulerClient) DownloadTorrent(name string) (io.ReadCloser, error) {
	var err error
	stopwatch := c.stats.SubScope("torrent").SubScope(name).Timer("download_time").Start()

	if !c.config.Enabled {
		return nil, errors.New("torrent not enabled")
	}

	var mi *torlib.MetaInfo
	if miRaw, err := c.store.GetDownloadOrCacheFileMeta(name); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to get file metainfo: %s", err)
		}
		mi, err = c.metaInfoClient.Get(name)
		if err != nil {
			return nil, fmt.Errorf("failed to get remote metainfo: %s", err)
		}
	} else {
		mi, err = torlib.NewMetaInfoFromBytes(miRaw)
		if err != nil {
			return nil, fmt.Errorf("failed to parse metainfo: %s", err)
		}
	}

	if _, err = c.archive.CreateTorrent(mi.InfoHash, mi); err != nil {
		return nil, fmt.Errorf("failed to create torrent in archive: %s", err)
	}

	select {
	case err := <-c.scheduler.AddTorrent(mi):
		if err != nil {
			return nil, fmt.Errorf("failed to schedule torrent: %s", err)
		}
	case <-time.After(downloadTimeout):
		// TODO(codyg): Allow cancelling the torrent in the Scheduler.
		return nil, fmt.Errorf("scheduled torrent timed out after %.2f seconds", downloadTimeout.Seconds())
	}

	stopwatch.Stop()
	return c.store.GetCacheFileReader(name)
}

// CreateTorrentFromFile creates a torrent from file and adds torrent to scheduler for seeding
func (c *SchedulerClient) CreateTorrentFromFile(name, filepath string) error {
	if !c.config.Enabled {
		log.Info("Torrent not enabled")
		return nil
	}

	mi, err := torlib.NewMetaInfoFromFile(
		name,
		filepath,
		int64(c.config.PieceLength),
		nil,
		"docker",
		"kraken-origin",
		"UTF-8")
	if err != nil {
		log.WithFields(log.Fields{
			"name":  name,
			"error": err,
		}).Error("Failed to create torrent")
		return err
	}

	miRaw, err := mi.Serialize()
	if err != nil {
		log.WithFields(log.Fields{
			"name":  name,
			"error": err,
		}).Error("Failed to create torrent")
		return err
	}

	ok, err := c.store.SetDownloadOrCacheFileMeta(name, []byte(miRaw))
	if err != nil {
		log.WithFields(log.Fields{
			"name":  name,
			"error": err,
		}).Error("Failed to create torrent")
		return err
	}

	if !ok {
		log.Warnf("%s_meta is already created", name)
	}

	_, err = c.archive.CreateTorrent(mi.InfoHash, mi)
	if err != nil {
		log.WithFields(log.Fields{
			"name":  name,
			"error": err,
		}).Error("Failed to create torrent")
	}

	if err := c.metaInfoClient.Post(mi); err != nil {
		log.WithFields(log.Fields{
			"name":  name,
			"error": err,
		}).Error("Failed to create torrent")
	}

	// create torrent from info
	errc := <-c.scheduler.AddTorrent(mi)
	if errc != nil {
		log.WithFields(bark.Fields{
			"name":     name,
			"infohash": mi.InfoHash,
			"error":    errc,
		}).Info("Failed to create torrent")
		return errc
	}

	log.WithFields(bark.Fields{
		"name":     name,
		"length":   mi.Info.Length,
		"infohash": mi.InfoHash,
		"origin":   c.peerID,
	}).Info("Successfully created torrent")

	return nil
}

// DropTorrent TODO
func (c *SchedulerClient) DropTorrent(infoHash torlib.InfoHash) error {
	// TODO
	return nil
}

// GetPeerContext returns peer context
func (c *SchedulerClient) GetPeerContext() (peercontext.PeerContext, error) {
	return c.pctx, nil
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
