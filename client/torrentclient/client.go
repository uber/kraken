package torrentclient

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken-torrent"
	"code.uber.internal/infra/kraken-torrent/bencode"
	"code.uber.internal/infra/kraken-torrent/metainfo"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/configuration"
	"code.uber.internal/infra/kraken/utils"
	"github.com/uber-common/bark"
)

const (
	downloadTimeout       = 120 //sec
	requestTimeout        = 5   //sec
	callTrackerRetries    = 3   //times
	callTrackerRetrySleep = 1   //sec
)

// Client contains a bittorent client and its
type Client struct {
	config *configuration.Config
	cl     *torrent.Client
}

// NewClient creates a new client
func NewClient(c *configuration.Config, s *store.LocalFileStore) (*Client, error) {
	torrentsManger := NewManager(c, s)
	client, err := torrent.NewClient(c.CreateAgentConfig(torrentsManger))
	if err != nil {
		return nil, err
	}

	return &Client{
		config: c,
		cl:     client,
	}, nil
}

// AddTorrent adds a torrent to the client given metainfo
func (c *Client) AddTorrent(mi *metainfo.MetaInfo) (*torrent.Torrent, error) {
	return c.cl.AddTorrent(mi)
}

// AddTorrentInfoHash adds a torrent to the client given infohash
func (c *Client) AddTorrentInfoHash(hash metainfo.Hash) (*torrent.Torrent, bool) {
	return c.cl.AddTorrentInfoHash(hash)
}

// AddTorrentMagnet adds a magnet to the client
// TODO (@evelynl): we dont need this anymore because essentially we only need infohash and an announcer
func (c *Client) AddTorrentMagnet(uri string) (*torrent.Torrent, error) {
	return c.cl.AddMagnet(uri)
}

// AddTorrentByName gets torrent info hash from tracker by name and adds the torrent to the client
// returns TorrentNotFound error if tracker does not have it
func (c *Client) AddTorrentByName(name string) (*torrent.Torrent, error) {
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
		announcer := c.config.Announce + "/announce"
		tor.AddTrackers([][]string{{announcer}})
	}

	log.WithFields(bark.Fields{
		"name":     name,
		"infohash": string(infohash[:]),
	}).Info("Successfully added torrent by name")

	return tor, nil
}

// Torrent gets a torrent from client given infohash
func (c *Client) Torrent(hash metainfo.Hash) (*torrent.Torrent, bool) {
	return c.cl.Torrent(hash)
}

// CreateTorrentFromFile creates a torrent and add it to the client
func (c *Client) CreateTorrentFromFile(name, filepath string) error {
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
	announcer := c.config.Announce + "/announce"
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
	c.cl.WriteStatus(w)
}

// TimedDownload downloads a torrent with a timeout
func (c *Client) TimedDownload(tor *torrent.Torrent) error {
	// check torrent info
	timer := time.NewTimer(downloadTimeout * time.Second)
	select {
	case <-timer.C:
		return fmt.Errorf("Timeout waiting for torrent info: %s", tor.Name())
	case <-tor.GotInfo():
	}

	// start download
	timer = time.NewTimer(downloadTimeout * time.Second)
	select {
	case <-timer.C:
		tor.Drop()
		return fmt.Errorf("Timeout downloading torrent: %s", tor.Name())
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
	trackerURL := c.config.Announce + "/infohash?name=" + name
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
	postURL := c.config.Announce + "/infohash?name=" + name + "&info_hash=" + infohash.HexString()

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
		ip, err = utils.GetHostIP()
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
