package scheduler

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/jackpal/bencode-go"
	"github.com/uber-common/bark"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/client/torrent/meta"
	"code.uber.internal/infra/kraken/client/torrent/storage"
	// TODO(codyg): Probably factor these into a common structs package?
	trackerservice "code.uber.internal/infra/kraken/tracker/service"
	trackerstorage "code.uber.internal/infra/kraken/tracker/storage"
)

// ErrTorrentAlreadyRegistered returns when adding a torrent which has already
// been added to the Scheduler.
var ErrTorrentAlreadyRegistered = errors.New("torrent already registered in scheduler")

// ErrSchedulerStopped returns when an action fails due to the Scheduler being stopped.
var ErrSchedulerStopped = errors.New("scheduler has been stopped")

type incomingHandshakeEvent struct {
	nc        net.Conn
	handshake *handshake
}

type failedHandshakeEvent struct {
	peerID   PeerID
	infoHash meta.Hash
}

type newConnEvent struct {
	conn    *conn
	torrent *torrent
}

type announceResponseEvent struct {
	infoHash meta.Hash
	peers    []trackerstorage.PeerInfo
}

type newTorrentEvent struct {
	torrent *torrent
	errc    chan error
}

type connKey struct {
	peerID   PeerID
	infoHash meta.Hash
}

func (k connKey) String() string {
	return fmt.Sprintf("connKey(peer=%s, hash=%s)", k.peerID, k.infoHash)
}

// Scheduler manages global state for the peer. This includes:
// - Opening torrents.
// - Announcing to the tracker.
// - Handshaking incoming connections.
// - Initializing outgoing connections.
// - Dispatching connections to torrents.
// - Pre-empting existing connections when better options are available (TODO).
type Scheduler struct {
	peerID     PeerID
	ip         string
	port       int
	datacenter string
	config     Config

	torrentManager storage.TorrentManager

	connFactory       *connFactory
	dispatcherFactory *dispatcherFactory

	dispatchers  map[meta.Hash]*dispatcher
	conns        map[connKey]*conn
	pendingConns map[connKey]bool

	listener net.Listener

	incomingHandshakes   chan *incomingHandshakeEvent
	failedHandshakes     chan *failedHandshakeEvent
	incomingConns        chan *newConnEvent
	outgoingConns        chan *newConnEvent
	closedConns          chan *conn
	newTorrents          chan *newTorrentEvent
	completedDispatchers chan *dispatcher

	announceQueue     *announceQueue
	announceTicker    *time.Ticker
	announceResponses chan *announceResponseEvent
	announceFailures  chan *dispatcher

	// The following fields orchestrate the stopping of the Scheduler.
	once sync.Once      // Ensures the stop sequence is executed only once.
	done chan struct{}  // Signals all goroutines to exit.
	wg   sync.WaitGroup // Waits for eventLoop and listenLoop to exit.
}

// New creates and starts a Scheduler. Incoming connections are accepted on the
// given ip / port, and the local peer is announced as part of the given datacenter.
func New(
	peerID PeerID,
	ip string,
	port int,
	datacenter string,
	tm storage.TorrentManager,
	config Config) (*Scheduler, error) {

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return nil, err
	}
	s := &Scheduler{
		peerID:               peerID,
		ip:                   ip,
		port:                 port,
		datacenter:           datacenter,
		config:               config,
		torrentManager:       tm,
		dispatchers:          make(map[meta.Hash]*dispatcher),
		conns:                make(map[connKey]*conn),
		pendingConns:         make(map[connKey]bool),
		listener:             l,
		incomingHandshakes:   make(chan *incomingHandshakeEvent),
		failedHandshakes:     make(chan *failedHandshakeEvent),
		incomingConns:        make(chan *newConnEvent),
		outgoingConns:        make(chan *newConnEvent),
		closedConns:          make(chan *conn),
		newTorrents:          make(chan *newTorrentEvent),
		completedDispatchers: make(chan *dispatcher),
		announceQueue:        newAnnounceQueue(),
		announceTicker:       time.NewTicker(config.AnnounceInterval),
		announceResponses:    make(chan *announceResponseEvent),
		announceFailures:     make(chan *dispatcher),
		done:                 make(chan struct{}),
	}
	s.connFactory = &connFactory{
		Config:      config,
		LocalPeerID: peerID,
		Closed:      s.connClosed,
	}
	s.dispatcherFactory = &dispatcherFactory{
		LocalPeerID: peerID,
	}

	s.start()

	return s, nil
}

// Stop shuts down the scheduler.
func (s *Scheduler) Stop() {
	s.log().Info("Stop called")
	s.once.Do(func() {
		close(s.done)
		s.wg.Wait()
	})
}

// AddTorrent starts downloading / seeding the torrent of the given hash. The returned
// error channel emits an error if the torrent failed to download, or nil once the
// torrent finishes downloading.
//
// TODO(codyg): Torrents will continue to seed for the entire lifetime of the Scheduler,
// but this should be a matter of policy.
func (s *Scheduler) AddTorrent(
	store storage.Torrent, infoHash meta.Hash, infoBytes []byte) <-chan error {

	// Buffer size of 1 so sends do not block.
	errc := make(chan error, 1)
	t, err := newTorrent(infoHash, infoBytes, store)
	if err != nil {
		errc <- err
		return errc
	}
	s.newTorrents <- &newTorrentEvent{t, errc}
	return errc
}

func (s *Scheduler) start() {
	s.wg.Add(2)
	go s.eventLoop()
	go s.listenLoop()
}

func (s *Scheduler) listenLoop() {
	for {
		nc, err := s.listener.Accept()
		if err != nil {
			// TODO Need some way to make this gracefully exit.
			s.log().Errorf("Failed to accept new connection: %s", err)
			break
		}
		s.log().Debug("New incoming connection")
		go s.handshakeIncomingConn(nc)
	}
	s.log().Debug("listenLoop exit")
	s.wg.Done()
}

func (s *Scheduler) handshakeIncomingConn(nc net.Conn) {
	h, err := receiveHandshake(nc)
	if err != nil {
		s.log().Errorf("Error receiving handshake from incoming connection: %s", err)
		nc.Close()
		return
	}
	select {
	case s.incomingHandshakes <- &incomingHandshakeEvent{nc, h}:
	case <-s.done:
	}
}

func (s *Scheduler) doInitIncomingConn(nc net.Conn, remoteHandshake *handshake) (*newConnEvent, error) {
	store, infoBytes, err := s.torrentManager.OpenTorrent(remoteHandshake.InfoHash)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to open torrent storage: %s", err)
	}
	t, err := newTorrent(remoteHandshake.InfoHash, infoBytes, store)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create torrent: %s", err)
	}
	c, err := s.connFactory.ReciprocateHandshake(
		nc, remoteHandshake, &handshake{s.peerID, remoteHandshake.InfoHash, t.Bitfield()})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to reciprocate handshake: %s", err)
	}
	return &newConnEvent{c, t}, nil
}

func (s *Scheduler) initIncomingConn(nc net.Conn, remoteHandshake *handshake) {
	s.logf(log.Fields{"peer": remoteHandshake.PeerID}).Debugf("Handshaking incoming connection")

	e, err := s.doInitIncomingConn(nc, remoteHandshake)
	if err != nil {
		s.logf(log.Fields{
			"handshake": remoteHandshake,
		}).Errorf("Error initializing incoming connection: %s", err)
		select {
		case s.failedHandshakes <- &failedHandshakeEvent{remoteHandshake.PeerID, remoteHandshake.InfoHash}:
		case <-s.done:
		}
		return
	}
	select {
	case s.incomingConns <- e:
	case <-s.done:
	}
}

func (s *Scheduler) doInitOutgoingConn(
	peerID PeerID, ip string, port int, t *torrent) (*newConnEvent, error) {

	addr := fmt.Sprintf("%s:%d", ip, port)
	nc, err := net.DialTimeout("tcp", addr, s.config.DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer: %s", err)
	}
	h := &handshake{s.peerID, t.InfoHash, t.Bitfield()}
	c, err := s.connFactory.SendAndReceiveHandshake(nc, h)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to handshake peer: %s", err)
	}
	if c.PeerID != peerID {
		c.Close()
		return nil, errors.New("unexpected peer id from handshaked conn")
	}
	return &newConnEvent{c, t}, nil
}

func (s *Scheduler) initOutgoingConn(peerID PeerID, ip string, port int, t *torrent) {
	s.logf(log.Fields{
		"peer": peerID, "ip": ip, "port": port, "torrent": t,
	}).Debug("Initializing outgoing connection")

	e, err := s.doInitOutgoingConn(peerID, ip, port, t)
	if err != nil {
		s.logf(log.Fields{
			"peer": peerID, "ip": ip, "port": port, "torrent": t,
		}).Errorf("Error intializing outgoing connection: %s", err)
		select {
		case s.failedHandshakes <- &failedHandshakeEvent{peerID, t.InfoHash}:
		case <-s.done:
		}
		return
	}
	select {
	case s.outgoingConns <- e:
	case <-s.done:
	}
}

func (s *Scheduler) doAnnounce(t *torrent) (*announceResponseEvent, error) {
	v := url.Values{}

	v.Add("info_hash", t.InfoHash.String())
	v.Add("peer_id", s.peerID.String())
	v.Add("port", strconv.Itoa(s.port))
	v.Add("ip", s.ip)
	v.Add("dc", s.datacenter)

	downloaded := t.Downloaded()
	v.Add("downloaded", strconv.FormatInt(downloaded, 10))
	v.Add("left", strconv.FormatInt(t.Length()-downloaded, 10))

	// TODO(codyg): Implement these last two arguments.
	v.Add("uploaded", "0")
	v.Add("event", "")

	req := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Host:     s.config.TrackerAddr,
			Scheme:   "http",
			Path:     "/announce",
			RawQuery: v.Encode(),
		},
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("request not ok: %d - %s", resp.StatusCode, b)
	}
	var ar trackerservice.AnnouncerResponse
	if err := bencode.Unmarshal(resp.Body, &ar); err != nil {
		return nil, fmt.Errorf("unmarshal failed: %s", err)
	}
	return &announceResponseEvent{
		infoHash: t.InfoHash,
		peers:    ar.Peers,
	}, nil
}

func (s *Scheduler) announce(d *dispatcher) {
	e, err := s.doAnnounce(d.Torrent)
	if err != nil {
		s.logf(log.Fields{"dispatcher": d}).Errorf("Announce failed: %s", err)
		select {
		case s.announceFailures <- d:
		case <-s.done:
		}
		return
	}
	select {
	case s.announceResponses <- e:
	case <-s.done:
	}
}

// getConnOpener returns the PeerID of the peer who opened the conn, i.e. sent the first handshake.
func (s *Scheduler) getConnOpener(c *conn) PeerID {
	if c.OpenedByRemote() {
		return c.PeerID
	}
	return s.peerID
}

// tryAddConn attempts to add a new connection to the Scheduler.
//
// If a connection already exists for this peer, we may preempt the existing connection. This
// is to prevent the case where two peers, A and B, both initialize connections to each other
// at the exact same time. If neither connection is tramsitting data yet, the peers independently
// agree on which connection should be kept by selecting the connection opened by the peer
// with the larger peer id.
func (s *Scheduler) tryAddConn(newConn *conn) error {
	k := connKey{newConn.PeerID, newConn.InfoHash}
	existingConn, ok := s.conns[k]
	if !ok {
		s.conns[k] = newConn
		return nil
	}

	existingOpener := s.getConnOpener(existingConn)
	newOpener := s.getConnOpener(newConn)

	newConnPreferred := existingOpener != newOpener &&
		!existingConn.Active() &&
		existingOpener.LessThan(newOpener)

	if newConnPreferred {
		existingConn.Close()
		s.conns[k] = newConn
		return nil
	}

	return errors.New("conn already exists")
}

func (s *Scheduler) handleClosedConn(c *conn) {
	s.logf(log.Fields{"conn": c}).Debug("Received closed conn")

	k := connKey{c.PeerID, c.InfoHash}
	// It is possible that we've received a closed conn after we've already
	// replaced it, so we need to make sure we're deleting the right one.
	if cur, ok := s.conns[k]; ok && cur == c {
		delete(s.conns, k)
	}
}

func (s *Scheduler) handleFailedHandshake(peerID PeerID, infoHash meta.Hash) {
	s.logf(log.Fields{"peer": peerID, "hash": infoHash}).Debug("Received failed handshake")

	delete(s.pendingConns, connKey{peerID, infoHash})
}

func (s *Scheduler) validateIncomingHandshake(h *handshake) error {
	d, ok := s.dispatchers[h.InfoHash]
	if ok && d.NumOpenConnections() >= s.config.MaxOpenConnectionsPerTorrent {
		return errors.New("dispatcher is at capacity")
	}
	k := connKey{h.PeerID, h.InfoHash}
	if _, ok := s.conns[k]; ok {
		return errors.New("active connection already exists")
	}
	if s.pendingConns[k] {
		return errors.New("pending connection already exists")
	}
	return nil
}

func (s *Scheduler) handleIncomingHandshake(nc net.Conn, h *handshake) {
	s.logf(log.Fields{"handshake": h}).Debug("Received incoming handshake")

	if err := s.validateIncomingHandshake(h); err != nil {
		s.logf(log.Fields{"handshake": h}).Errorf("Rejecting incoming handshake: %s", err)
		nc.Close()
		return
	}
	s.pendingConns[connKey{h.PeerID, h.InfoHash}] = true
	go s.initIncomingConn(nc, h)
}

func (s *Scheduler) addIncomingConn(c *conn, t *torrent) error {
	delete(s.pendingConns, connKey{c.PeerID, c.InfoHash})
	if err := s.tryAddConn(c); err != nil {
		return fmt.Errorf("cannot add conn to scheduler: %s", err)
	}
	d, ok := s.dispatchers[t.InfoHash]
	if !ok {
		d = s.dispatcherFactory.New(t, s.dispatcherCompleteFn(nil))
		s.dispatchers[t.InfoHash] = d
	}
	if err := d.AddConn(c); err != nil {
		return fmt.Errorf("cannot add conn to dispatcher: %s", err)
	}
	return nil
}

func (s *Scheduler) handleIncomingConn(c *conn, t *torrent) {
	s.logf(log.Fields{"conn": c, "torrent": t}).Debug("Received incoming conn")

	if err := s.addIncomingConn(c, t); err != nil {
		s.logf(log.Fields{
			"conn": c, "torrent": t,
		}).Errorf("Error adding incoming conn: %s", err)
		c.Close()
	}
}

func (s *Scheduler) addOutgoingConn(c *conn, t *torrent) error {
	delete(s.pendingConns, connKey{c.PeerID, c.InfoHash})
	d, ok := s.dispatchers[t.InfoHash]
	if !ok {
		// We should have created the dispatcher before sending a handshake.
		return errors.New("no dispatcher found")
	}
	if err := s.tryAddConn(c); err != nil {
		return fmt.Errorf("cannot add conn to scheduler: %s", err)
	}
	if err := d.AddConn(c); err != nil {
		return fmt.Errorf("cannot add conn to dispatcher: %s", err)
	}
	return nil
}

func (s *Scheduler) handleOutgoingConn(c *conn, t *torrent) {
	s.logf(log.Fields{"conn": c, "torrent": t}).Debug("Received outgoing conn")

	if err := s.addOutgoingConn(c, t); err != nil {
		s.logf(log.Fields{
			"conn": c, "torrent": t,
		}).Errorf("Error adding outgoing conn: %s", err)
		c.Close()
	}
}

func (s *Scheduler) handleAnnounceTick() {
	s.log().Debug("Received announce tick")

	d, ok := s.announceQueue.Next()
	if !ok {
		s.log().Debug("No dispatchers in announce queue")
		return
	}
	s.logf(log.Fields{"dispatcher": d}).Debug("Announcing")
	go s.announce(d)
}

func (s *Scheduler) handleAnnounceResponse(infoHash meta.Hash, peers []trackerstorage.PeerInfo) {
	s.logf(log.Fields{"hash": infoHash, "peers": peers}).Debug("Received announce result")

	d, ok := s.dispatchers[infoHash]
	if !ok {
		s.logf(log.Fields{
			"hash": infoHash,
		}).Info("Dispatcher closed after announce response received")
		return
	}
	s.announceQueue.Ready(d)
	maxOpen := s.config.MaxOpenConnectionsPerTorrent
	for i, opened := 0, d.NumOpenConnections(); i < len(peers) && opened < maxOpen; i++ {
		p := peers[i]
		pid, err := NewPeerID(p.PeerID)
		if err != nil {
			s.logf(log.Fields{
				"peer": p.PeerID, "hash": infoHash,
			}).Errorf("Error creating PeerID from announce response: %s", err)
			continue
		}
		if pid == s.peerID {
			// Tracker may return our own peer.
			continue
		}
		k := connKey{pid, infoHash}
		if s.pendingConns[k] {
			s.logf(log.Fields{"key": k}).Info("Peer already has pending connection, skipping")
			continue
		}
		if _, ok := s.conns[k]; ok {
			s.logf(log.Fields{"key": k}).Info("Peer already has opened connection, skipping")
			continue
		}
		s.pendingConns[k] = true
		go s.initOutgoingConn(pid, p.IP, int(p.Port), d.Torrent)
		opened++
	}
}

func (s *Scheduler) handleAnnounceFailure(d *dispatcher) {
	s.logf(log.Fields{"dispatcher": d}).Debug("Received announce failure")

	s.announceQueue.Ready(d)
}

func (s *Scheduler) handleNewTorrent(t *torrent, errc chan error) {
	s.logf(log.Fields{"torrent": t}).Debug("Received new torrent")

	if _, ok := s.dispatchers[t.InfoHash]; ok {
		s.logf(log.Fields{
			"torrent": t,
		}).Info("Skipping torrent, info hash already registered in Scheduler")
		errc <- ErrTorrentAlreadyRegistered
		return
	}
	d := s.dispatcherFactory.New(t, s.dispatcherCompleteFn(errc))
	s.dispatchers[t.InfoHash] = d
	s.announceQueue.Add(d)
}

func (s *Scheduler) handleCompletedDispatcher(d *dispatcher) {
	s.logf(log.Fields{"dispatcher": d}).Debug("Received completed dispatcher")

	s.announceQueue.Done(d)
}

// eventLoop handles events from the various channels of Scheduler, providing synchronization to
// all Scheduler state.
func (s *Scheduler) eventLoop() {
	s.log().Debugf("Starting eventLoop")
	for {
		select {
		case c := <-s.closedConns:
			s.handleClosedConn(c)

		case e := <-s.failedHandshakes:
			s.handleFailedHandshake(e.peerID, e.infoHash)

		case e := <-s.incomingHandshakes:
			s.handleIncomingHandshake(e.nc, e.handshake)

		case e := <-s.incomingConns:
			s.handleIncomingConn(e.conn, e.torrent)

		case e := <-s.outgoingConns:
			s.handleOutgoingConn(e.conn, e.torrent)

		case <-s.announceTicker.C:
			s.handleAnnounceTick()

		case e := <-s.announceResponses:
			s.handleAnnounceResponse(e.infoHash, e.peers)

		case d := <-s.announceFailures:
			s.handleAnnounceFailure(d)

		case e := <-s.newTorrents:
			s.handleNewTorrent(e.torrent, e.errc)

		case d := <-s.completedDispatchers:
			s.handleCompletedDispatcher(d)

		case <-s.done:
			s.log().Debug("Received done event")
			s.listener.Close()
			for _, c := range s.conns {
				c.Close()
			}
			s.wg.Done()
			s.log().Debug("eventLoop exit")
			return
		}
	}
}

// dispatcherCompleteFn returns a closure to be called when a dispatcher finishes
// downloading a torrent. errc is an optional error channel which signals whether
// the dispatcher was able to successfully complete or not.
func (s *Scheduler) dispatcherCompleteFn(errc chan error) func(*dispatcher) {
	return func(d *dispatcher) {
		go func() {
			select {
			case s.completedDispatchers <- d:
				if errc != nil {
					errc <- nil
				}
			case <-s.done:
				if errc != nil {
					errc <- ErrSchedulerStopped
				}
			}
		}()
	}
}

func (s *Scheduler) connClosed(c *conn) {
	go func() {
		select {
		case s.closedConns <- c:
		case <-s.done:
		}
	}()
}

func (s *Scheduler) logf(f log.Fields) bark.Logger {
	f["scheduler"] = s.peerID
	return log.WithFields(f)
}

func (s *Scheduler) log() bark.Logger {
	return s.logf(log.Fields{})
}
