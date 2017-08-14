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
	host       string
	port       string
	datacenter string
	config     Config

	torrentManager storage.TorrentManager

	connFactory       *connFactory
	dispatcherFactory *dispatcherFactory

	// The following fields define the core Scheduler "state", and should only
	// be accessed from within the event loop.
	dispatchers   map[meta.Hash]*dispatcher // Active seeding / leeching torrents.
	connState     *connState
	announceQueue *announceQueue
	torrentErrors map[meta.Hash]chan error // AddTorrent error channels.

	eventLoop *eventLoop

	listener net.Listener

	announceTicker         *time.Ticker
	preemptionTicker       *time.Ticker
	blacklistCleanupTicker *time.Ticker

	// The following fields orchestrate the stopping of the Scheduler.
	once sync.Once      // Ensures the stop sequence is executed only once.
	done chan struct{}  // Signals all goroutines to exit.
	wg   sync.WaitGroup // Waits for eventLoop and listenLoop to exit.
}

// New creates and starts a Scheduler. Incoming connections are accepted on the
// addr, and the local peer is announced as part of the datacenter.
func New(
	peerID PeerID,
	addr string,
	datacenter string,
	tm storage.TorrentManager,
	config Config) (*Scheduler, error) {

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	eventLoop := newEventLoop(done)
	s := &Scheduler{
		peerID:         peerID,
		host:           host,
		port:           port,
		datacenter:     datacenter,
		config:         config,
		torrentManager: tm,
		connFactory: &connFactory{
			Config:      config,
			LocalPeerID: peerID,
			EventLoop:   eventLoop,
		},
		dispatcherFactory: &dispatcherFactory{
			Config:      config,
			LocalPeerID: peerID,
			EventLoop:   eventLoop,
		},
		dispatchers:            make(map[meta.Hash]*dispatcher),
		connState:              newConnState(peerID, config),
		announceQueue:          newAnnounceQueue(),
		torrentErrors:          make(map[meta.Hash]chan error),
		eventLoop:              eventLoop,
		listener:               l,
		announceTicker:         time.NewTicker(config.AnnounceInterval),
		preemptionTicker:       time.NewTicker(config.PreemptionInterval),
		blacklistCleanupTicker: time.NewTicker(config.BlacklistCleanupInterval),
		done: done,
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

		// Waits for all loops to stop.
		s.wg.Wait()

		for _, c := range s.connState.ActiveConns() {
			c.Close()
		}

		// Notify local clients of pending torrents that they will not complete.
		for _, errc := range s.torrentErrors {
			errc <- ErrSchedulerStopped
		}

		s.log().Info("Stop complete")
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
	s.eventLoop.Send(newTorrentEvent{t, errc})
	return errc
}

func (s *Scheduler) start() {
	s.wg.Add(3)
	go s.runEventLoop()
	go s.listenLoop()
	go s.tickerLoop()
}

// eventLoop handles eventLoop from the various channels of Scheduler, providing synchronization to
// all Scheduler state.
func (s *Scheduler) runEventLoop() {
	s.log().Debugf("Starting eventLoop")
	s.eventLoop.Run(s)
	s.log().Debug("eventLoop done")
	s.wg.Done()
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

// tickerLoop periodically emits various tick events.
func (s *Scheduler) tickerLoop() {
	for {
		select {
		case <-s.announceTicker.C:
			s.eventLoop.Send(announceTickEvent{})
		case <-s.preemptionTicker.C:
			s.eventLoop.Send(preemptionTickEvent{})
		case <-s.blacklistCleanupTicker.C:
			s.eventLoop.Send(cleanupBlacklistEvent{})
		case <-s.done:
			s.log().Debug("tickerLoop done")
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
	s.eventLoop.Send(incomingHandshakeEvent{nc, h})
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
	s.eventLoop.Send(e)
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
	s.eventLoop.Send(e)
}

func (s *Scheduler) doAnnounce(t *torrent) ([]trackerstorage.PeerInfo, error) {
	v := url.Values{}

	v.Add("info_hash", t.InfoHash.String())
	v.Add("peer_id", s.peerID.String())
	v.Add("port", s.port)
	v.Add("ip", s.host)
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
	s.eventLoop.Send(e)
}

func (s *Scheduler) addOutgoingConn(c *conn, t *torrent) error {
	if err := s.connState.MovePendingToActive(c); err != nil {
		c.Close()
		return fmt.Errorf("cannot add conn to scheduler: %s", err)
	}
	d, ok := s.dispatchers[t.InfoHash]
	if !ok {
		// We should have created the dispatcher before sending a handshake.
		return errors.New("no dispatcher found")
	}
	if err := d.AddConn(c); err != nil {
		return fmt.Errorf("cannot add conn to dispatcher: %s", err)
	}
	return nil
}

func (s *Scheduler) addIncomingConn(c *conn, t *torrent) error {
	if err := s.connState.MovePendingToActive(c); err != nil {
		c.Close()
		return fmt.Errorf("cannot add conn to scheduler: %s", err)
	}
	d, ok := s.dispatchers[t.InfoHash]
	if !ok {
		d = s.dispatcherFactory.New(t)
		s.dispatchers[t.InfoHash] = d
	}
	if err := d.AddConn(c); err != nil {
		return fmt.Errorf("cannot add conn to dispatcher: %s", err)
	}
	return nil
}

func (s *Scheduler) logf(f log.Fields) bark.Logger {
	f["scheduler"] = s.peerID
	return log.WithFields(f)
}

func (s *Scheduler) log() bark.Logger {
	return s.logf(log.Fields{})
}
