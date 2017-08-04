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

type connKey struct {
	peerID   PeerID
	infoHash meta.Hash
}

func (k connKey) String() string {
	return fmt.Sprintf("connKey(peer=%s, hash=%s)", k.peerID, k.infoHash)
}

// event describes an external event which moves the Scheduler into a new state.
// While the event is applying, it is guaranteed to be the only accessor of
// Scheduler state.
type event interface {
	Apply(s *Scheduler)
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

	// The following fields define the core Scheduler "state", and should only
	// be accessed from within the event loop.
	dispatchers   map[meta.Hash]*dispatcher // Active seeding / leeching torrents.
	conns         map[connKey]*conn         // Active connections.
	pendingConns  map[connKey]bool          // Pending connections.
	announceQueue *announceQueue

	events         chan event
	listener       net.Listener
	announceTicker *time.Ticker

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
		peerID:         peerID,
		ip:             ip,
		port:           port,
		datacenter:     datacenter,
		config:         config,
		torrentManager: tm,
		dispatchers:    make(map[meta.Hash]*dispatcher),
		conns:          make(map[connKey]*conn),
		pendingConns:   make(map[connKey]bool),
		announceQueue:  newAnnounceQueue(),
		events:         make(chan event),
		listener:       l,
		announceTicker: time.NewTicker(config.AnnounceInterval),
		done:           make(chan struct{}),
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
		s.listener.Close()
		s.wg.Wait() // Waits for all loops to stop.
		for _, c := range s.conns {
			c.Close()
		}
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
	s.events <- &newTorrentEvent{t, errc}
	return errc
}

// emit sends a new event into the event loop. Should never be called by the event loop
// goroutine (which handles Apply methods), else deadlock will occur.
func (s *Scheduler) emit(e event) {
	select {
	case s.events <- e:
	case <-s.done:
	}
}

func (s *Scheduler) start() {
	s.wg.Add(3)
	go s.eventLoop()
	go s.listenLoop()
	go s.announceTickerLoop()
}

// eventLoop handles events from the various channels of Scheduler, providing synchronization to
// all Scheduler state.
func (s *Scheduler) eventLoop() {
	s.log().Debugf("Starting eventLoop")
	for {
		select {
		case e := <-s.events:
			e.Apply(s)
		case <-s.done:
			s.log().Debug("eventLoop done")
			s.wg.Done()
			return
		}
	}
}

// listenLoop accepts incoming connections.
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
	s.log().Debug("listenLoop done")
	s.wg.Done()
}

// announceTickerLoop periodically emits announceTickEvents.
func (s *Scheduler) announceTickerLoop() {
	for {
		select {
		case <-s.announceTicker.C:
			s.emit(announceTickEvent{})
		case <-s.done:
			s.log().Debug("announceTickerLoop done")
			s.wg.Done()
			return
		}
	}
}

func (s *Scheduler) handshakeIncomingConn(nc net.Conn) {
	h, err := receiveHandshake(nc)
	if err != nil {
		s.log().Errorf("Error receiving handshake from incoming connection: %s", err)
		nc.Close()
		return
	}
	s.emit(incomingHandshakeEvent{nc, h})
}

func (s *Scheduler) doInitIncomingConn(
	nc net.Conn, remoteHandshake *handshake) (*conn, *torrent, error) {

	store, infoBytes, err := s.torrentManager.OpenTorrent(remoteHandshake.InfoHash)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("failed to open torrent storage: %s", err)
	}
	t, err := newTorrent(remoteHandshake.InfoHash, infoBytes, store)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("failed to create torrent: %s", err)
	}
	c, err := s.connFactory.ReciprocateHandshake(
		nc, remoteHandshake, &handshake{s.peerID, remoteHandshake.InfoHash, t.Bitfield()})
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("failed to reciprocate handshake: %s", err)
	}
	return c, t, nil
}

func (s *Scheduler) initIncomingConn(nc net.Conn, remoteHandshake *handshake) {
	s.logf(log.Fields{"peer": remoteHandshake.PeerID}).Debugf("Handshaking incoming connection")

	var e event
	c, t, err := s.doInitIncomingConn(nc, remoteHandshake)
	if err != nil {
		s.logf(log.Fields{
			"handshake": remoteHandshake,
		}).Errorf("Error initializing incoming connection: %s", err)
		e = failedHandshakeEvent{remoteHandshake.PeerID, remoteHandshake.InfoHash}
	} else {
		e = incomingConnEvent{c, t}
	}
	s.emit(e)
}

func (s *Scheduler) doInitOutgoingConn(
	peerID PeerID, ip string, port int, t *torrent) (*conn, error) {

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
	return c, nil
}

func (s *Scheduler) initOutgoingConn(peerID PeerID, ip string, port int, t *torrent) {
	s.logf(log.Fields{
		"peer": peerID, "ip": ip, "port": port, "torrent": t,
	}).Debug("Initializing outgoing connection")

	var e event
	c, err := s.doInitOutgoingConn(peerID, ip, port, t)
	if err != nil {
		s.logf(log.Fields{
			"peer": peerID, "ip": ip, "port": port, "torrent": t,
		}).Errorf("Error intializing outgoing connection: %s", err)
		e = failedHandshakeEvent{peerID, t.InfoHash}
	} else {
		e = outgoingConnEvent{c, t}
	}
	s.emit(e)
}

func (s *Scheduler) doAnnounce(t *torrent) ([]trackerstorage.PeerInfo, error) {
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
	return ar.Peers, nil
}

func (s *Scheduler) announce(d *dispatcher) {
	var e event
	peers, err := s.doAnnounce(d.Torrent)
	if err != nil {
		s.logf(log.Fields{"dispatcher": d}).Errorf("Announce failed: %s", err)
		e = announceFailureEvent{d}
	} else {
		e = announceResponseEvent{d.Torrent.InfoHash, peers}
	}
	s.emit(e)
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

// dispatcherCompleteFn returns a closure to be called when a dispatcher finishes
// downloading a torrent. errc is an optional error channel which signals whether
// the dispatcher was able to successfully complete or not.
func (s *Scheduler) dispatcherCompleteFn(errc chan error) func(*dispatcher) {
	return func(d *dispatcher) {
		go func() {
			select {
			case s.events <- completedDispatcherEvent{d}:
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
	s.emit(closedConnEvent{c})
}

func (s *Scheduler) logf(f log.Fields) bark.Logger {
	f["scheduler"] = s.peerID
	return log.WithFields(f)
}

func (s *Scheduler) log() bark.Logger {
	return s.logf(log.Fields{})
}

// closedConnEvent occurs when a connection is closed.
type closedConnEvent struct {
	conn *conn
}

// Apply ejects the conn from the Scheduler's active connections.
func (e closedConnEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"conn": e.conn}).Debug("Applying closed conn event")

	k := connKey{e.conn.PeerID, e.conn.InfoHash}
	// It is possible that we've received a closed conn after we've already
	// replaced it, so we need to make sure we're deleting the right one.
	if cur, ok := s.conns[k]; ok && cur == e.conn {
		delete(s.conns, k)
	}
}

// failedHandshakeEvent occurs when a pending connection fails to handshake.
type failedHandshakeEvent struct {
	peerID   PeerID
	infoHash meta.Hash
}

// Apply ejects the peer/hash of the failed handshake from the Scheduler's
// pending connections.
func (e failedHandshakeEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"peer": e.peerID, "hash": e.infoHash}).Debug("Applying failed handshake event")

	delete(s.pendingConns, connKey{e.peerID, e.infoHash})
}

// incomingHandshakeEvent when a handshake was received from a new connection.
type incomingHandshakeEvent struct {
	nc        net.Conn
	handshake *handshake
}

// Apply rejects incoming handshakes when the Scheduler is at capacity. If the
// Scheduler has capacity for more connections, adds the peer/hash of the handshake
// to the Scheduler's pending connections and asynchronously attempts to establish
// the connection.
func (e incomingHandshakeEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"handshake": e.handshake}).Debug("Applying incoming handshake event")

	if err := s.validateIncomingHandshake(e.handshake); err != nil {
		s.logf(log.Fields{"handshake": e.handshake}).Errorf("Rejecting incoming handshake: %s", err)
		e.nc.Close()
		return
	}
	s.pendingConns[connKey{e.handshake.PeerID, e.handshake.InfoHash}] = true
	go s.initIncomingConn(e.nc, e.handshake)
}

// incomingConnEvent occurs when a pending incoming connection finishes handshaking.
type incomingConnEvent struct {
	conn    *conn
	torrent *torrent
}

// Apply transitions a fully-handshaked incoming conn from pending to active.
func (e incomingConnEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"conn": e.conn, "torrent": e.torrent}).Debug("Applying incoming conn event")

	if err := s.addIncomingConn(e.conn, e.torrent); err != nil {
		s.logf(log.Fields{
			"conn": e.conn, "torrent": e.torrent,
		}).Errorf("Error adding incoming conn: %s", err)
		e.conn.Close()
	}
}

// outgoingConnEvent occurs when a pending outgoing connection finishes handshaking.
type outgoingConnEvent struct {
	conn    *conn
	torrent *torrent
}

// Apply transitions a fully-handshaked outgoing conn from pending to active.
func (e outgoingConnEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"conn": e.conn, "torrent": e.torrent}).Debug("Applying outgoing conn event")

	if err := s.addOutgoingConn(e.conn, e.torrent); err != nil {
		s.logf(log.Fields{
			"conn": e.conn, "torrent": e.torrent,
		}).Errorf("Error adding outgoing conn: %s", err)
		e.conn.Close()
	}
}

// announceTickEvent occurs when it is time to announce to the tracker.
type announceTickEvent struct{}

// Apply pulls the next dispatcher from the announce queue and asynchronously
// makes an announce request to the tracker.
func (e announceTickEvent) Apply(s *Scheduler) {
	s.log().Debug("Applying announce tick event")

	d, ok := s.announceQueue.Next()
	if !ok {
		s.log().Debug("No dispatchers in announce queue")
		return
	}
	s.logf(log.Fields{"dispatcher": d}).Debug("Announcing")
	go s.announce(d)
}

// announceResponseEvent occurs when a successfully announce response was received
// from the tracker.
type announceResponseEvent struct {
	infoHash meta.Hash
	peers    []trackerstorage.PeerInfo
}

// Apply selects new peers returned via an announce response to open connections to
// if there is capacity. These connections are added to the Scheduler's pending
// connections and handshaked asynchronously.
//
// Also marks the dispatcher as ready to announce again.
func (e announceResponseEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"hash": e.infoHash, "peers": e.peers}).Debug("Applying announce response event")

	d, ok := s.dispatchers[e.infoHash]
	if !ok {
		s.logf(log.Fields{
			"hash": e.infoHash,
		}).Info("Dispatcher closed after announce response received")
		return
	}
	s.announceQueue.Ready(d)
	maxOpen := s.config.MaxOpenConnectionsPerTorrent
	for i, opened := 0, d.NumOpenConnections(); i < len(e.peers) && opened < maxOpen; i++ {
		p := e.peers[i]
		pid, err := NewPeerID(p.PeerID)
		if err != nil {
			s.logf(log.Fields{
				"peer": p.PeerID, "hash": e.infoHash,
			}).Errorf("Error creating PeerID from announce response: %s", err)
			continue
		}
		if pid == s.peerID {
			// Tracker may return our own peer.
			continue
		}
		k := connKey{pid, e.infoHash}
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

// announceFailureEvent occurs when an announce request fails.
type announceFailureEvent struct {
	dispatcher *dispatcher
}

// Apply marks the dispatcher as ready to announce again.
func (e announceFailureEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"dispatcher": e.dispatcher}).Debug("Applying announce failure event")

	s.announceQueue.Ready(e.dispatcher)
}

// newTorrentEvent occurs when a new torrent was requested for download.
type newTorrentEvent struct {
	torrent *torrent
	errc    chan error
}

// Apply begins seeding / leeching a new torrent.
func (e newTorrentEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"torrent": e.torrent}).Debug("Applying new torrent event")

	if _, ok := s.dispatchers[e.torrent.InfoHash]; ok {
		s.logf(log.Fields{
			"torrent": e.torrent,
		}).Info("Skipping torrent, info hash already registered in Scheduler")
		e.errc <- ErrTorrentAlreadyRegistered
		return
	}
	d := s.dispatcherFactory.New(e.torrent, s.dispatcherCompleteFn(e.errc))
	s.dispatchers[e.torrent.InfoHash] = d
	s.announceQueue.Add(d)
}

// completedDispatcherEvent occurs when a dispatcher finishes downloading its torrent.
type completedDispatcherEvent struct {
	dispatcher *dispatcher
}

// Apply marks the dispatcher for its final announce.
func (e completedDispatcherEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"dispatcher": e.dispatcher}).Debug("Applying completed dispatcher event")

	s.announceQueue.Done(e.dispatcher)
}
