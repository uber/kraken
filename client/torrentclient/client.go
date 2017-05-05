package torrentclient

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken-torrent"
	"code.uber.internal/infra/kraken-torrent/bencode"
	"code.uber.internal/infra/kraken-torrent/metainfo"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/configuration"
	"code.uber.internal/infra/kraken/utils"
	"github.com/docker/distribution/uuid"
	"github.com/uber-common/bark"
)

const (
	uploadTimeout         = 120 //sec
	downloadTimeout       = 120 //sec
	requestTimeout        = 5   //sec
	callTrackerRetries    = 3   //times
	callTrackerRetrySleep = 1   //sec
)

// Client contains a bittorent client and its
type Client struct {
	config  *configuration.Config
	store   *store.LocalFileStore
	cl      *torrent.Client
	timeout int //sec
}

// NewClient creates a new client
func NewClient(c *configuration.Config, s *store.LocalFileStore, t int) (*Client, error) {
	if c.DisableTorrent {
		log.Info("Torrent disabled")
		return &Client{
			config: c,
			store:  s,
			cl:     nil,
		}, nil
	}

	torrentsManager := NewManager(c, s)
	client, err := torrent.NewClient(c.CreateAgentConfig(torrentsManager))
	if err != nil {
		return nil, err
	}

	return &Client{
		config:  c,
		store:   s,
		cl:      client,
		timeout: t,
	}, nil
}

// AddTorrent adds a torrent to the client given metainfo
func (c *Client) AddTorrent(mi *metainfo.MetaInfo) (*torrent.Torrent, error) {
	if c.config.DisableTorrent {
		return nil, fmt.Errorf("Torrent disabled")
	}
	return c.cl.AddTorrent(mi)
}

// AddTorrentInfoHash adds a torrent to the client given infohash
func (c *Client) AddTorrentInfoHash(hash metainfo.Hash) (*torrent.Torrent, bool, error) {
	if c.config.DisableTorrent {
		return nil, false, fmt.Errorf("Torrent disabled")
	}
	tor, new := c.cl.AddTorrentInfoHash(hash)
	return tor, new, nil
}

// AddTorrentByName gets torrent info hash from tracker by name and adds the torrent to the client
// returns TorrentNotFound error if tracker does not have it
func (c *Client) AddTorrentByName(name string) (*torrent.Torrent, error) {
	if c.config.DisableTorrent {
		return nil, fmt.Errorf("Torrent disabled")
	}
	infohash, err := c.getTorrentInfoHashFromTracker(name)
	if err != nil {
		log.WithFields(bark.Fields{
			"name":  name,
			"error": err,
		}).Error("Failed to add torrent by name")
		return nil, err
	}

	// get local peer
	localPeer, err := c.getLocalPeer()
	if err != nil {
		log.WithFields(bark.Fields{
			"name":  name,
			"error": err,
		}).Info("Failed to add torrent by name")
		return nil, err
	}

	// add torrent
	tor, new := c.cl.AddTorrentInfoHash(metainfo.NewHashFromHex(string(infohash[:])))

	if new {
		// add itself as peer
		tor.AddPeers([]torrent.Peer{localPeer})

		// add announcer
		announcer := c.config.TrackerURL + "/announce"
		tor.AddTrackers([][]string{{announcer}})
	}

	log.WithFields(bark.Fields{
		"name":     name,
		"infohash": string(infohash[:]),
	}).Info("Successfully added torrent by name")

	return tor, nil
}

// Torrent gets a torrent from client given infohash
func (c *Client) Torrent(hash metainfo.Hash) (*torrent.Torrent, bool, error) {
	if c.config.DisableTorrent {
		return nil, false, fmt.Errorf("Torrent disabled")
	}

	tor, ok := c.cl.Torrent(hash)
	return tor, ok, nil
}

// CreateTorrentFromFile creates a torrent and add it to the client
// called by dockerregistry.Uploads and Tags
func (c *Client) CreateTorrentFromFile(name, filepath string) error {
	if c.config.DisableTorrent {
		log.Info("Torrent disabled. Nothing is to be done here")
		return nil
	}
	// build info hash from file
	info := metainfo.Info{
		Name:        name,
		PieceLength: int64(c.config.Agent.PieceLength),
	}
	err := info.BuildFromFilePath(filepath)
	if err != nil {
		log.WithFields(bark.Fields{
			"name":  name,
			"error": err,
		}).Info("Failed to create torrent")
		return err
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		log.WithFields(bark.Fields{
			"name":  name,
			"error": err,
		}).Info("Failed to create torrent")
		return err
	}

	mi := &metainfo.MetaInfo{
		InfoBytes: infoBytes,
	}

	// create torrent from info
	t, err := c.cl.AddTorrent(mi)
	if err != nil {
		log.WithFields(bark.Fields{
			"name":     t.Name(),
			"infohash": t.InfoHash().HexString(),
			"error":    err,
		}).Info("Failed to create torrent")
		return err
	}

	localPeer, err := c.getLocalPeer()
	if err != nil {
		log.WithFields(bark.Fields{
			"name":     t.Name(),
			"infohash": t.InfoHash().HexString(),
			"error":    err,
		}).Info("Failed to create torrent")
		return err
	}

	// add torrent name in tracker
	err = c.addTorrentInTracker(name, mi.HashInfoBytes())
	if err != nil {
		log.WithFields(bark.Fields{
			"name":     t.Name(),
			"infohash": t.InfoHash().HexString(),
			"error":    err,
		}).Info("Failed to create torrent")
		return err
	}

	// add itself as peer
	t.AddPeers([]torrent.Peer{localPeer})

	// add announcer
	announcer := c.config.TrackerURL + "/announce"
	t.AddTrackers([][]string{{announcer}})

	log.WithFields(bark.Fields{
		"name":      t.Name(),
		"length":    t.Length(),
		"infohash":  t.InfoHash(),
		"origin":    localPeer.IP,
		"announcer": announcer,
	}).Info("Successfully created torrent")

	return nil
}

// WriteStatus writes torrent client status to a writer
func (c *Client) WriteStatus(w io.Writer) {
	if c.config.DisableTorrent {
		w.Write([]byte("Torrent disabled"))
		return
	}
	c.cl.WriteStatus(w)
}

// PostManifest saves manifest specified by the tag it referred in a tracker
func (c *Client) PostManifest(repo, tag, manifest string) error {
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

	req, err := http.NewRequest("POST", postURL, reader)
	if err != nil {
		return err
	}

	client := http.Client{
		Timeout: uploadTimeout * time.Second, // sec
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respDump, err := httputil.DumpResponse(resp, true)
		if err != nil {
			return err
		}
		return fmt.Errorf("%s", respDump)
	}

	return nil
}

// GetManifest gets manifest from a tracker, it returns manifest digest
func (c *Client) GetManifest(repo, tag string) (string, error) {
	if c.config.DisableTorrent {
		return "", fmt.Errorf("Torrent disabled")
	}

	log.Infof("torrentclient GetManifest %s:%s", repo, tag)
	name := fmt.Sprintf("%s:%s", repo, tag)
	getURL := c.config.TrackerURL + "/manifest/" + url.QueryEscape(name)

	req, err := http.NewRequest("GET", getURL, nil)
	if err != nil {
		return "", err
	}

	client := http.Client{
		Timeout: downloadTimeout * time.Second, // sec
	}

	// send request
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respDump, err := httputil.DumpResponse(resp, true)
		if err != nil {
			return "", err
		}
		return "", fmt.Errorf("%s", respDump)
	}

	// read manifest from body
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// parse manifest
	_, manifestDigest, err := utils.ParseManifestV2(data)
	if err != nil {
		return "", err
	}

	log.Infof("torrentclient GetManifest digest:%s", manifestDigest)
	// store manifest
	// TODO (@evelynl): create an upload file instead of a download file because
	// we want to allow storing the same manifest by multiple threads,
	// whichever successfully move the file to cache will succeed.
	// the reason that we cannot use a download file is because we cannot rename the file
	// when the file is moved to cache. so it might cause race condition when multiple threads are writing the same manifest.
	manifestDigestTemp := manifestDigest + "." + uuid.Generate().String()
	_, err = c.store.CreateUploadFile(manifestDigestTemp, 0)
	if err != nil {
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

// DownloadByName adds and downloads torrent by name
// called by dockerregistry.Blobs and Tags
func (c *Client) DownloadByName(name string) error {
	if c.config.DisableTorrent {
		return fmt.Errorf("Torrent disabled")
	}

	tor, err := c.AddTorrentByName(name)
	if err != nil {
		return err
	}
	return c.Download(tor)
}

// Download downloads a torrent with a timeout
func (c *Client) Download(tor *torrent.Torrent) error {
	if c.config.DisableTorrent {
		return fmt.Errorf("Torrent disabled")
	}

	// check torrent info
	timer := time.NewTimer(time.Duration(c.timeout) * time.Second)
	select {
	case <-timer.C:
		return fmt.Errorf("Timeout waiting for torrent info: %s. Exceeds %d seconds", tor.Name(), c.timeout)
	case <-tor.GotInfo():
	}

	// start download
	timer = time.NewTimer(time.Duration(c.timeout) * time.Second)
	select {
	case <-timer.C:
		tor.Drop()
		return fmt.Errorf("Timeout downloading torrent: %s. Exceeds %d seconds", tor.Name(), c.timeout)
	case <-c.download(tor):
		log.Infof("Sucessfully downloaded torrent %s", tor.Name())
		return nil
	}
}

func (c *Client) download(tor *torrent.Torrent) <-chan byte {
	completedPieces := 0
	psc := tor.SubscribePieceStateChanges()
	tor.DownloadAll()
	ch := make(chan byte, 1)
	go func() {
		for {
			select {
			case v := <-psc.Values:
				if v.(torrent.PieceStateChange).Complete {
					completedPieces = completedPieces + 1
				}
				if completedPieces == tor.NumPieces() {
					ch <- 'c'
				}
			}
		}
	}()
	return ch
}

func (c *Client) getTorrentInfoHashFromTracker(name string) ([]byte, error) {
	// get torrent info hash
	trackerURL := c.config.TrackerURL + "/infohash?name=" + name
	req, err := http.NewRequest("GET", trackerURL, nil)
	if err != nil {
		return nil, err
	}

	client := http.Client{
		Timeout: requestTimeout * time.Second, // sec
	}

	// send request
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	log.Infof("Query %s, status code %s", req.URL.String(), resp.Status)

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

func (c *Client) addTorrentInTracker(name string, infohash metainfo.Hash) (err error) {
	postURL := c.config.TrackerURL + "/infohash?name=" + name + "&info_hash=" + infohash.HexString()

	for i := 0; i < callTrackerRetries; i++ {
		var req *http.Request
		var resp *http.Response
		req, err = http.NewRequest("POST", postURL, nil)
		if err != nil {
			log.Errorf("Failed to add torrent in tracker: %s", err.Error())
			time.Sleep(callTrackerRetrySleep * time.Second)
			continue
		}

		client := http.Client{Timeout: 5 * time.Second}
		resp, err = client.Do(req)
		if err != nil {
			log.Errorf("Failed to add torrent in tracker: %s", err.Error())
			time.Sleep(callTrackerRetrySleep * time.Second)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			respDump, err := httputil.DumpResponse(resp, true)
			if err != nil {
				log.Errorf("Failed to add torrent in tracker: %s", err.Error())
				time.Sleep(callTrackerRetrySleep * time.Second)
				continue
			}
			err = fmt.Errorf("%s", respDump)
			log.Errorf("Failed to create torrent in tracker: %s", respDump)
			time.Sleep(callTrackerRetrySleep * time.Second)
		} else {
			log.Info("Sucessfully added torrent in tracker")
			return nil
		}
	}
	return
}

// getLocalPeer
func (c *Client) getLocalPeer() (torrent.Peer, error) {
	var ip string
	if c.config.Environment == "development" {
		ip = "127.0.0.1"
	} else {
		var err error
		ip, err = utils.GetLocalIP()
		if err != nil {
			return torrent.Peer{}, err
		}
	}

	return torrent.Peer{
		IP:   net.ParseIP(ip),
		Port: c.config.Agent.Backend,
	}, nil
}

// TorrentNotFoundError means the torrent cannot be found
type TorrentNotFoundError struct {
	Name string
	Msg  string
}

func (e *TorrentNotFoundError) Error() string {
	return fmt.Sprintf("Torrent not found: %s. %s", e.Name, e.Msg)
}

// IsTorrentNotFoundError returns true if the param is of TorrentNotFoundError type.
func IsTorrentNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	switch err.(type) {
	default:
		return false
	case *TorrentNotFoundError:
		return true
	}
}
