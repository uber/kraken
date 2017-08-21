package torrent

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/docker/distribution/uuid"
	"github.com/uber-common/bark"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrent/scheduler"
	"code.uber.internal/infra/kraken/client/torrent/storage"
	"code.uber.internal/infra/kraken/configuration"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils"
)

const requestTimeout = 60 * time.Second
const downloadTimeout = 120 * time.Second

// Client TODO
type Client interface {
	DownloadTorrent(name string) error
	CreateTorrentFromFile(name, filepath string) error
	GetManifest(repo, tag string) (string, error)
	PostManifest(repo, tag, manifestDigest string) error
	Close() error
}

// SchedulerClient is a client for scheduler
type SchedulerClient struct {
	config    *configuration.Config
	peerID    torlib.PeerID
	scheduler *scheduler.Scheduler

	// TODO: Consolidate these...
	store   *store.LocalStore
	archive storage.TorrentArchive
}

// NewSchedulerClient creates a new scheduler client
func NewSchedulerClient(config *configuration.Config, store *store.LocalStore) (Client, error) {
	peerID := torlib.PeerIDFixture()
	// TODO: Move this to .yaml
	schedulerConfig := scheduler.Config{
		TrackerAddr:                  config.TrackerURL,
		MaxOpenConnectionsPerTorrent: 20,
		AnnounceInterval:             500 * time.Millisecond,
		DialTimeout:                  5 * time.Second,
		WriteTimeout:                 5 * time.Second,
		SenderBufferSize:             0,
		ReceiverBufferSize:           0,
		IdleSeederTTL:                2 * time.Second,
		PreemptionInterval:           500 * time.Millisecond,
		IdleConnTTL:                  1 * time.Second,
		ConnTTL:                      5 * time.Minute,
		InitialBlacklistExpiration:   time.Second,
		BlacklistExpirationBackoff:   2,
		MaxBlacklistExpiration:       10 * time.Second,
		ExpiredBlacklistEntryTTL:     5 * time.Minute,
		BlacklistCleanupInterval:     time.Minute,
	}
	archive := storage.NewLocalTorrentArchive(store)
	scheduler, err := scheduler.New(peerID, fmt.Sprintf("127.0.0.1:%d", config.Agent.TorrentClientPort), "sjc1", archive, schedulerConfig)
	if err != nil {
		return nil, err
	}
	return &SchedulerClient{
		config:    config,
		peerID:    peerID,
		scheduler: scheduler,
		store:     store,
		archive:   archive,
	}, nil
}

// Close stops scheduler
func (c *SchedulerClient) Close() error {
	c.scheduler.Stop()
	return nil
}

// DownloadTorrent downloads a torrent given torrent name
func (c *SchedulerClient) DownloadTorrent(name string) error {
	if c.config.DisableTorrent {
		return fmt.Errorf("Torrent disabled")
	}

	var mi *torlib.MetaInfo
	miRaw, err := c.store.GetDownloadOrCacheFileMeta(name)
	if err != nil && !os.IsNotExist(err) {
		log.WithFields(log.Fields{
			"name":  name,
			"error": err,
		}).Error("Failed to download torrent")
		return err
	}

	if err == nil {
		var err error
		mi, err = torlib.NewMetaInfoFromBytes(miRaw)
		if err != nil {
			log.WithFields(log.Fields{
				"name":  name,
				"error": err,
			}).Error("Failed to download torrent")
			return err
		}
	}

	if err != nil && os.IsNotExist(err) {
		var err error
		mi, err = c.getTorrentMetaInfo(name)
		if err != nil {
			log.WithFields(log.Fields{
				"name":  name,
				"error": err,
			}).Error("Failed to download torrent")
			return err
		}
	}

	_, err = c.archive.CreateTorrent(mi.InfoHash, mi)
	if err != nil {
		log.WithFields(log.Fields{
			"name":  name,
			"error": err,
		}).Error("Failed to download torrent")
	}

	select {
	case errc := <-c.scheduler.AddTorrent(mi):
		if errc != nil {
			log.WithFields(log.Fields{
				"name":  name,
				"error": errc,
			}).Error("Failed to download torrent")
			return errc
		}
	case <-time.After(downloadTimeout):
		err := fmt.Errorf("Download timeout")
		log.WithFields(log.Fields{
			"name":  name,
			"error": err,
		}).Error("Failed to download torrent")
		return err
	}

	log.WithFields(log.Fields{
		"name": name,
	}).Info("Successfully downloaded torrent")
	return nil
}

// CreateTorrentFromFile creates a torrent from file and adds torrent to scheduler for seeding
func (c *SchedulerClient) CreateTorrentFromFile(name, filepath string) error {
	if c.config.DisableTorrent {
		log.Info("Torrent disabled")
		return nil
	}

	announce := path.Join(c.config.TrackerURL, "/announce")

	mi, err := torlib.NewMetaInfoFromFile(
		name,
		filepath,
		int64(c.config.Agent.PieceLength),
		[][]string{{c.config.TrackerURL + "/announce"}},
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
		"announce": announce,
	}).Info("Successfully created torrent")

	return nil
}

// DropTorrent TODO
func (c *SchedulerClient) DropTorrent(infoHash torlib.InfoHash) error {
	// TODO
	return nil
}

// GetManifest queries tracker for manifest and stores manifest locally
func (c *SchedulerClient) GetManifest(repo, tag string) (string, error) {
	if c.config.DisableTorrent {
		return "", fmt.Errorf("Torrent disabled")
	}
	name := fmt.Sprintf("%s:%s", repo, tag)

	trackerURL := c.config.TrackerURL + "/manifest/" + url.QueryEscape(name)
	data, err := c.sendRequestToTracker("GET", trackerURL, nil)
	if err != nil {
		return "", err
	}

	// parse manifest
	_, manifestDigest, err := utils.ParseManifestV2(data)
	if err != nil {
		return "", err
	}

	// Store manifest
	manifestDigestTemp := manifestDigest + "." + uuid.Generate().String()
	if err = c.store.CreateUploadFile(manifestDigestTemp, 0); err != nil {
		return "", err
	}

	writer, err := c.store.GetUploadFileReadWriter(manifestDigestTemp)
	if err != nil {
		return "", err
	}

	_, err = writer.Write(data)
	if err != nil {
		return "", err
	}
	writer.Close()

	err = c.store.MoveUploadFileToCache(manifestDigestTemp, manifestDigest)
	// it is ok if move fails on file exist error
	if err != nil && !os.IsExist(err) {
		return "", err
	}

	return manifestDigest, nil
}

// PostManifest saves manifest specified by the tag it referred in a tracker
func (c *SchedulerClient) PostManifest(repo, tag, manifest string) error {
	if c.config.DisableTorrent {
		log.Info("Torrent disabled. Nothing is to be done here")
		return nil
	}

	reader, err := c.store.GetCacheFileReader(manifest)
	if err != nil {
		return err
	}

	name := fmt.Sprintf("%s:%s", repo, tag)
	postURL := c.config.TrackerURL + "/manifest/" + url.QueryEscape(name)
	_, err = c.sendRequestToTracker("POST", postURL, reader)
	if err != nil {
		return err
	}

	return nil
}

func (c *SchedulerClient) getTorrentMetaInfo(name string) (*torlib.MetaInfo, error) {
	// get torrent info hash
	trackerURL := c.config.TrackerURL + "/info?name=" + name
	miRaw, err := c.sendRequestToTracker("GET", trackerURL, nil)
	if err != nil {
		return nil, err
	}

	mi, err := torlib.NewMetaInfoFromBytes(miRaw)
	if err != nil {
		return nil, err
	}
	return mi, nil
}

func (c *SchedulerClient) sendRequestToTracker(method, endpoint string, body io.Reader) ([]byte, error) {
	if body == nil {
		body = bytes.NewReader([]byte{})
	}

	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return nil, err
	}

	client := http.Client{
		Timeout: requestTimeout,
	}

	// send request
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respDump, err := httputil.DumpResponse(resp, true)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%s", respDump)
	}

	// read infohash from respsonse
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}
