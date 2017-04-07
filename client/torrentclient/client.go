package torrentclient

import (
	"net"
	"path"

	"io"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/configuration"
	"code.uber.internal/infra/kraken/utils"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/uber-common/bark"
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
		return err
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		return err
	}

	mi := &metainfo.MetaInfo{
		InfoBytes: infoBytes,
	}

	// Create torrent from info
	t, err := c.cl.AddTorrent(mi)
	if err != nil {
		return err
	}

	localPeer, err := c.getLocalPeer()
	if err != nil {
		return err
	}

	// Add itself as peer
	t.AddPeers([]torrent.Peer{localPeer})

	// Add announcer
	announcer := path.Join(c.config.Announce, "/announce")
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
