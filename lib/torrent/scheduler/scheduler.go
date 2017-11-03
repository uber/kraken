package scheduler

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/announceclient"
)

// Scheduler errors.
var (
	ErrTorrentAlreadyRegistered = errors.New("torrent already registered in scheduler")
	ErrSchedulerStopped         = errors.New("scheduler has been stopped")
	ErrTorrentCancelled         = errors.New("torrent has been cancelled")
)

// torrentControl bundles torrent control structures.
type torrentControl struct {
	Errors     []chan error
	Dispatcher *dispatcher
	Complete   bool
}

func newTorrentControl(d *dispatcher) *torrentControl {
	return &torrentControl{Dispatcher: d}
}

// Scheduler manages global state for the peer. This includes:
// - Opening torrents.
// - Announcing to the tracker.
// - Handshaking incoming connections.
// - Initializing outgoing connections.
// - Dispatching connections to torrents.
// - Pre-empting existing connections when better options are available (TODO).
type Scheduler struct {
	pctx           peercontext.PeerContext
	config         Config
	clock          clock.Clock
	torrentArchive storage.TorrentArchive
	stats          tally.Scope

	connFactory       *connFactory
	dispatcherFactory *dispatcherFactory

	// The following fields define the core Scheduler "state", and should only
	// be accessed from within the event loop.
	torrentControls map[torlib.InfoHash]*torrentControl // Active seeding / leeching torrents.
	connState       *connState
	announceQueue   *announceQueue

	eventLoop eventLoop

	listener net.Listener

	announceTick         <-chan time.Time
	preemptionTick       <-chan time.Time
	blacklistCleanupTick <-chan time.Time
	emitStatsTick        <-chan time.Time

	announceClient announceclient.Client

	networkEventProducer networkevent.Producer

	// The following fields orchestrate the stopping of the Scheduler.
	once sync.Once      // Ensures the stop sequence is executed only once.
	done chan struct{}  // Signals all goroutines to exit.
	wg   sync.WaitGroup // Waits for eventLoop and listenLoop to exit.
}

// schedOverrides defines Scheduler fields which may be overrided for testing
// purposes.
type schedOverrides struct {
	clock     clock.Clock
	eventLoop eventLoop
}

type option func(*schedOverrides)

func withClock(c clock.Clock) option {
	return func(o *schedOverrides) { o.clock = c }
}

func withEventLoop(l eventLoop) option {
	return func(o *schedOverrides) { o.eventLoop = l }
}

// New creates and starts a Scheduler.
func New(
	config Config,
	ta storage.TorrentArchive,
	stats tally.Scope,
	pctx peercontext.PeerContext,
	announceClient announceclient.Client,
	networkEventProducer networkevent.Producer,
	options ...option) (*Scheduler, error) {

	config = config.applyDefaults()

	log.Infof("Scheduler initializing with config:\n%s", config)

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", pctx.Port))
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	stats = stats.SubScope("scheduler")

	overrides := schedOverrides{
		clock:     clock.New(),
		eventLoop: newEventLoop(),
	}
	for _, opt := range options {
		opt(&overrides)
	}

	var preemptionTick <-chan time.Time
	if !config.DisablePreemption {
		preemptionTick = overrides.clock.Tick(config.PreemptionInterval)
	}
	var blacklistCleanupTick <-chan time.Time
	if !config.ConnState.DisableBlacklist {
		blacklistCleanupTick = overrides.clock.Tick(config.BlacklistCleanupInterval)
	}

	log.Infof("Scheduler will announce as peer %s on addr %s:%d",
		pctx.PeerID, pctx.IP, pctx.Port)

	s := &Scheduler{
		pctx:           pctx,
		config:         config,
		clock:          overrides.clock,
		torrentArchive: ta,
		stats:          stats,
		connFactory: &connFactory{
			Config:      config.Conn,
			LocalPeerID: pctx.PeerID,
			EventSender: overrides.eventLoop,
			Clock:       overrides.clock,
			Stats:       stats,
		},
		dispatcherFactory: &dispatcherFactory{
			Config:               config,
			LocalPeerID:          pctx.PeerID,
			EventSender:          overrides.eventLoop,
			Clock:                overrides.clock,
			NetworkEventProducer: networkEventProducer,
		},
		torrentControls:      make(map[torlib.InfoHash]*torrentControl),
		connState:            newConnState(pctx.PeerID, config.ConnState, overrides.clock),
		announceQueue:        newAnnounceQueue(),
		eventLoop:            overrides.eventLoop,
		listener:             l,
		announceTick:         overrides.clock.Tick(config.AnnounceInterval),
		preemptionTick:       preemptionTick,
		blacklistCleanupTick: blacklistCleanupTick,
		emitStatsTick:        overrides.clock.Tick(config.EmitStatsInterval),
		announceClient:       announceClient,
		networkEventProducer: networkEventProducer,
		done:                 done,
	}

	if config.DisablePreemption {
		s.log().Warn("Preemption disabled")
	}
	if config.ConnState.DisableBlacklist {
		s.log().Warn("Blacklisting disabled")
	}
	if config.Conn.DisableThrottling {
		s.log().Warn("Throttling disabled")
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
		s.eventLoop.Stop()

		// Waits for all loops to stop.
		s.wg.Wait()

		for _, c := range s.connState.ActiveConns() {
			c.Close()
		}

		// Notify local clients of pending torrents that they will not complete.
		for _, ctrl := range s.torrentControls {
			for _, errc := range ctrl.Errors {
				errc <- ErrSchedulerStopped
			}
		}

		s.log().Info("Stop complete")
	})
}

// AddTorrent starts downloading / seeding the torrent given metainfo. The returned
// error channel emits an error if it failed to get torrent from archive
//
// TODO(codyg): Torrents will continue to seed for the entire lifetime of the Scheduler,
// but this should be a matter of policy.
func (s *Scheduler) AddTorrent(mi *torlib.MetaInfo) <-chan error {
	// Buffer size of 1 so sends do not block.
	errc := make(chan error, 1)

	t, err := s.torrentArchive.CreateTorrent(mi)
	if err != nil {
		errc <- fmt.Errorf("create torrent: %s", err)
		return errc
	}

	if !s.eventLoop.Send(newTorrentEvent{t, errc}) {
		errc <- ErrSchedulerStopped
		return errc
	}

	return errc
}

// CancelTorrent stops downloading the torrent of h.
func (s *Scheduler) CancelTorrent(h torlib.InfoHash) {
	s.eventLoop.Send(cancelTorrentEvent{h})
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
	s.log().Infof("Listening on %s", s.listener.Addr().String())
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
		case <-s.announceTick:
			s.eventLoop.Send(announceTickEvent{})
		case <-s.preemptionTick:
			s.eventLoop.Send(preemptionTickEvent{})
		case <-s.blacklistCleanupTick:
			s.eventLoop.Send(cleanupBlacklistEvent{})
		case <-s.emitStatsTick:
			s.eventLoop.Send(emitStatsEvent{})
		case <-s.done:
			s.log().Debug("tickerLoop done")
			s.wg.Done()
			return
		}
	}
}

func (s *Scheduler) handshakeIncomingConn(nc net.Conn) {
	h, err := receiveHandshake(nc, s.config.Conn.HandshakeTimeout)
	if err != nil {
		s.log().Errorf("Error receiving handshake from incoming connection: %s", err)
		nc.Close()
		return
	}
	s.eventLoop.Send(incomingHandshakeEvent{nc, h})
}

func (s *Scheduler) doInitIncomingConn(
	nc net.Conn, remoteHandshake *handshake) (*conn, storage.Torrent, error) {

	t, err := s.torrentArchive.GetTorrent(remoteHandshake.Name, remoteHandshake.InfoHash)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("failed to open torrent storage: %s", err)
	}
	c, err := s.connFactory.ReciprocateHandshake(nc, t, remoteHandshake)
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
	peerID torlib.PeerID, ip string, port int, t storage.Torrent) (*conn, error) {

	addr := fmt.Sprintf("%s:%d", ip, port)
	nc, err := net.DialTimeout("tcp", addr, s.config.DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer: %s", err)
	}
	c, err := s.connFactory.SendAndReceiveHandshake(nc, t)
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

func (s *Scheduler) initOutgoingConn(peerID torlib.PeerID, ip string, port int, t storage.Torrent) {
	s.logf(log.Fields{
		"peer": peerID, "ip": ip, "port": port, "torrent": t,
	}).Debug("Initializing outgoing connection")

	var e event
	c, err := s.doInitOutgoingConn(peerID, ip, port, t)
	if err != nil {
		s.logf(log.Fields{
			"peer": peerID, "ip": ip, "port": port, "torrent": t,
		}).Errorf("Error intializing outgoing connection: %s", err)
		e = failedHandshakeEvent{peerID, t.InfoHash()}
	} else {
		e = outgoingConnEvent{c, t}
	}
	s.eventLoop.Send(e)
}

func (s *Scheduler) announce(d *dispatcher) {
	var e event
	peers, err := s.announceClient.Announce(
		d.Torrent.Name(), d.Torrent.InfoHash(), d.Torrent.BytesDownloaded())
	if err != nil {
		s.logf(log.Fields{"dispatcher": d}).Errorf("Announce failed: %s", err)
		e = announceFailureEvent{d}
	} else {
		e = announceResponseEvent{d.Torrent.InfoHash(), peers}
	}
	s.eventLoop.Send(e)
}

func (s *Scheduler) addOutgoingConn(c *conn, t storage.Torrent) error {
	if err := s.connState.MovePendingToActive(c); err != nil {
		c.Close()
		return fmt.Errorf("cannot add conn to scheduler: %s", err)
	}
	ctrl, ok := s.torrentControls[t.InfoHash()]
	if !ok {
		return errors.New("torrent must be created before sending handshake")
	}
	if err := ctrl.Dispatcher.AddConn(c); err != nil {
		return fmt.Errorf("cannot add conn to dispatcher: %s", err)
	}
	return nil
}

func (s *Scheduler) addIncomingConn(c *conn, t storage.Torrent) error {
	if err := s.connState.MovePendingToActive(c); err != nil {
		c.Close()
		return fmt.Errorf("cannot add conn to scheduler: %s", err)
	}
	ctrl, ok := s.torrentControls[t.InfoHash()]
	if !ok {
		ctrl = s.initTorrentControl(t)
	}
	if err := ctrl.Dispatcher.AddConn(c); err != nil {
		return fmt.Errorf("cannot add conn to dispatcher: %s", err)
	}
	return nil
}

// initTorrentControl initializes a new torrentControl for t. Overwrites any
// existing torrentControl for t, so callers should check if one exists first.
func (s *Scheduler) initTorrentControl(t storage.Torrent) *torrentControl {
	ctrl := newTorrentControl(s.dispatcherFactory.New(t))
	s.announceQueue.Add(ctrl.Dispatcher)
	s.networkEventProducer.Produce(networkevent.AddTorrentEvent(t.InfoHash(), s.pctx.PeerID, t.Bitfield()))
	s.torrentControls[t.InfoHash()] = ctrl
	return ctrl
}

func (s *Scheduler) logf(f log.Fields) bark.Logger {
	return log.WithFields(f)
}

func (s *Scheduler) log() bark.Logger {
	return s.logf(log.Fields{})
}
