package torrent

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/client/torrent/meta"
	"code.uber.internal/infra/kraken/client/torrent/storage"
	"code.uber.internal/infra/kraken/utils"
)

const (
	bep20                             = "-GT0001-"
	minDialTimeout                    = 5 * time.Second
	handshakesTimeout                 = 20 * time.Second
	defaultEstablishedConnsPerTorrent = 80
)

// Client manageas torrents and general P2P configuration
type Client struct {
	config Config

	peerID         [20]byte
	defaultStorage storage.TorrentStorage
	tcpListener    net.Listener

	listenAddr string

	torrents map[meta.Hash]*Torrent
	sync.RWMutex
}

// Spec specifies a new torrent for adding to a client.
type Spec struct {
	// The tiered tracker URIs.
	Trackers  [][]string
	InfoHash  meta.Hash
	InfoBytes []byte
	// The name to use if the Name field from the Info isn't available.
	DisplayName string
}

// SpecFromMetaInfo Generates a torrent's spec by meta info
func SpecFromMetaInfo(mi *meta.TorrentInfo) (spec *Spec) {
	info, _ := mi.UnmarshalInfo()
	spec = &Spec{
		Trackers:    mi.AnnounceList,
		InfoBytes:   mi.InfoBytes,
		DisplayName: info.Name,
		InfoHash:    mi.HashInfoBytes(),
	}
	if spec.Trackers == nil && mi.Announce != "" {
		spec.Trackers = [][]string{{mi.Announce}}
	}
	return
}

// PeerID returns client's peer id
func (cl *Client) PeerID() string {
	return string(cl.peerID[:])
}

type torrentAddr string

func (torrentAddr) Network() string {
	return ""
}

func (ta torrentAddr) String() string {
	return string(ta)
}

// ListenAddr returns client's listening address
func (cl *Client) ListenAddr() net.Addr {
	if cl.listenAddr == "" {
		return nil
	}
	return torrentAddr(cl.listenAddr)
}

func listenTCP(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}

// NewClient creates a new client.
func NewClient(cfg *Config) (cl *Client, err error) {
	if cfg == nil {
		cfg = &Config{}
	}

	defer func() {
		if err != nil {
			cl = nil
		}
	}()
	cl = &Client{
		config:     *cfg,
		torrents:   make(map[meta.Hash]*Torrent),
		listenAddr: cfg.ListenAddr,
	}
	storageImpl := cfg.DefaultStorage
	if storageImpl == nil {
		storageImpl = storage.NewFileStorage(cfg.DataDir)
	}
	cl.defaultStorage = storageImpl

	rand.Seed(time.Now().UTC().UnixNano())
	if cfg.PeerID != "" {
		copy(cl.peerID[:], cfg.PeerID)
	} else {
		o := copy(cl.peerID[:], bep20)
		_, err = rand.Read(cl.peerID[o:])
		if err != nil {
			panic("error generating peer id")
		}
	}

	log.Debugf("client peerID: %s", hex.EncodeToString(cl.peerID[:]))

	cl.tcpListener, err = listenTCP(cl.listenAddr)
	if err != nil {
		log.Errorf("Cannot create a client: %s", err)
		return
	}
	if cl.tcpListener != nil {
		log.Debugf("accepting connections on %s", cl.ListenAddr())
		go cl.acceptConnections(cl.tcpListener)
	}
	return
}

// HasTorrent returns true if a torrent's hash registered in a client
func (cl *Client) HasTorrent(ihash string) bool {
	_, ok := cl.torrents[meta.NewHashFromHex(ihash)]
	return ok
}

// GetTorrent returns a client's torrent or nil if does not exist
func (cl *Client) GetTorrent(ihash string) *Torrent {
	if t, ok := cl.torrents[meta.NewHashFromHex(ihash)]; ok {
		return t
	}
	return nil
}

// Close stops the client. All connections to peers are closed and all activity will
// come to a halt.
func (cl *Client) Close() {
	if cl.tcpListener != nil {
		cl.tcpListener.Close()
	}

	for _, t := range cl.torrents {
		t.Close()
	}
}

func (cl *Client) acceptConnections(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Errorf("connection: accept failure: %s", err)
			return
		}
		log.Debugf("accepted connection from %s", conn.RemoteAddr())
		go cl.incomingConnection(conn)
	}
}

func (cl *Client) incomingConnection(nc net.Conn) {
	defer nc.Close()
	if tc, ok := nc.(*net.TCPConn); ok {
		tc.SetLinger(0)
	}
	c := cl.newConnection(nc, nil)
	cl.runReceivedConn(c)
}

type dialResult struct {
	Conn net.Conn
}

// Start the process of connecting to the given peer for the given torrent if
// appropriate.
func (cl *Client) initiateConn(peer Peer, t *Torrent) {
	if peer.ID == cl.peerID {
		log.Errorf("cannot initiate connection to itself: %s", hex.EncodeToString(cl.peerID[:]))
		return
	}
	addr := net.JoinHostPort(peer.IP.String(), fmt.Sprintf("%d", peer.Port))

	log.Debugf("opening outgoing connection for torrent %s to a peer: %s", t.infoHash, peer)
	go cl.outgoingConnection(t, addr)
}

// Performs initiator handshakes and returns a connection. Returns nil
// *connection if no connection for valid reasons.
func (cl *Client) handshakeConnection(nc net.Conn, t *Torrent) (*Connection, error) {
	c := cl.newConnection(nc, t)

	ih, err := c.handshake(&t.infoHash, cl.peerID[:], t.bitfield())
	if ih != nil && *ih != t.infoHash {
		log.Error("Unexpected torrent hash info in handshaking phase")
		return nil, err
	}
	log.Debugf("new connection handshaked for a torrent %s", t.infoHash)
	return c, err
}

// Called to dial out and run a connection. The addr we're given is already
// considered half-open.
func (cl *Client) outgoingConnection(t *Torrent, addr string) {
	log.Debugf("outgoing connection for a torrent %s to %s", t.infoHash, addr)

	nc, err := net.DialTimeout("tcp", addr, minDialTimeout)
	if err != nil {
		log.Error("error dialing for connection: %s", err)
		return
	}

	nc.(*net.TCPConn).SetLinger(0)

	defer nc.Close()

	log.Debugf("handshaking connection for a torrent %s", t.infoHash)
	c, err := cl.handshakeConnection(nc, t)
	if err != nil || c == nil {
		log.Error("handshake error: closing the connection")
		nc.Close()
	}

	if err != nil {
		if cl.config.Debug {
			log.Errorf("error establishing outgoing connection: %s", err)
		}
		return
	}
	if c == nil {
		return
	}
	if c.PeerID == cl.peerID {
		log.Errorf("Client and connection peer ids are the same %s, cannot connect to itself",
			hex.EncodeToString(cl.peerID[:]))
		return
	}

	defer t.dropConnection(c)

	if !t.addConnection(c) {
		log.Errorf("connection outgoing: could not add a connection to a connection pool")
		return
	}

	//start a writing loop
	go c.Writer()

	//start a reading loop
	go c.Reader()

	// notify an updater
	t.updateCh <- struct{}{}

	<-c.done
	<-c.done
}

// The port number for incoming peer connections. 0 if the client isn't
// listening.
func (cl *Client) incomingPeerPort() int {
	if cl.listenAddr == "" {
		return 0
	}
	port, err := utils.AddrPort(cl.listenAddr)
	if err != nil {
		log.Errorf("incomingPeerPort: could not parse an incoming addr string: %s, err: %s",
			cl.listenAddr, err)
		return -1
	}
	return port
}

type (
	peerID [20]byte
)

func (cl *Client) runReceivedConn(c *Connection) {

	//nil indicates we don't initiate a handshaking, merely handle the incoming request
	ihash, err := c.handshake(nil, cl.peerID[:], nil)
	if err != nil {
		log.Errorf("connection received: failed handshaking: %s", err)
		return
	}

	if ihash == nil {
		log.Errorf("handshake: unexpected ihash(nil) response in a receiver: %s", err)
		return
	}

	//handshake handles the case when info hash is not found locally, could it be racy?
	t := cl.torrents[*ihash]

	if c.PeerID == cl.peerID {
		log.Errorf("connection received: cannot connect to itself")
		return
	}

	defer t.dropConnection(c)

	if !t.addConnection(c) {
		log.Errorf("connection received: could not add a connection to a connection pool")
		return
	}

	//start a writing loop
	go c.Writer()

	//start a reading loop
	go c.Reader()

	// notify an updater
	t.updateCh <- struct{}{}

	<-c.done
	<-c.done
}

func (cl *Client) openNewConns(t *Torrent) bool {
	log.Debugf("opening new connections for torrent %s", t.infoHash.String())
	for len(t.peers) != 0 {
		if !t.wantConns() {
			log.Infof("torrent does not want to open new connecttions, has data already: %s", t.String())
			return false
		}
		var (
			k peersKey
			p Peer
		)
		for k, p = range t.peers {
			break
		}
		delete(t.peers, k)
		cl.initiateConn(p, t)
	}
	return true
}

// Return a Torrent ready for insertion into a Client.
func (cl *Client) newTorrent(ih meta.Hash, storage storage.TorrentStorage) (*Torrent, error) {
	return NewTorrent(cl, ih, nil, storage, defaultEstablishedConnsPerTorrent)
}

//AddTorrentInfoHash adds torren's info hash
func (cl *Client) AddTorrentInfoHash(infoHash meta.Hash) (t *Torrent, new bool) {
	return cl.AddTorrentInfoHashWithStorage(infoHash, nil)
}

// AddTorrentInfoHashWithStorage adds a torrent by InfoHash with a custom Storage implementation.
// If the torrent already exists then this Storage is ignored and the
// existing torrent returned with `new` set to `false`
func (cl *Client) AddTorrentInfoHashWithStorage(infoHash meta.Hash, specStorage storage.TorrentStorage) (*Torrent, bool) {
	t, ok := cl.torrents[infoHash]
	if ok {
		return t, ok
	}
	var err error
	if t, err = cl.newTorrent(infoHash, specStorage); err != nil {
		log.Errorf("client cannot create a new torrent: %s", err)
		return nil, false
	}

	cl.torrents[infoHash] = t
	//TODO: issue a request to a torrent announcer to get peers
	return t, true
}

// AddTorrentSpec adds or merge a torrent spec.
func (cl *Client) AddTorrentSpec(spec *Spec) (t *Torrent, new bool, err error) {
	t, new = cl.AddTorrentInfoHashWithStorage(spec.InfoHash, cl.config.DefaultStorage)
	if spec.InfoBytes != nil {
		err = t.SetInfoBytes(spec.InfoBytes)
		if err != nil {
			log.Errorf("cannot set infobytes to a torrent: %s", err)
			return nil, false, err
		}
	}

	// TODO: get a list of peers from announcer
	t.openNewConns()
	return
}

func (cl *Client) dropTorrent(infoHash meta.Hash) (err error) {
	t, ok := cl.torrents[infoHash]
	if !ok {
		err = fmt.Errorf("no such torrent")
		return
	}
	err = t.Close()
	if err != nil {
		panic(err)
	}
	delete(cl.torrents, infoHash)
	return
}

// AddTorrent adds fully qualified torrent to a client
func (cl *Client) AddTorrent(mi *meta.TorrentInfo) (T *Torrent, err error) {
	t, _, err := cl.AddTorrentSpec(SpecFromMetaInfo(mi))
	return t, err
}

// AddTorrentFromFile adds fully qualified torrent to a client from a file
func (cl *Client) AddTorrentFromFile(filename string) (T *Torrent, err error) {
	mi, err := meta.LoadFromFile(filename)
	if err != nil {
		return
	}
	return cl.AddTorrent(mi)
}

func (cl *Client) newConnection(nc net.Conn, t *Torrent) *Connection {
	c, _ := NewConnection(t, cl, nc, 250, 0)
	return c
}
