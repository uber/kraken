package scheduler

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/utils/log"
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
	announceQueue   announceQueue

	eventLoop eventLoop

	listener net.Listener

	announceTick         <-chan time.Time
	preemptionTick       <-chan time.Time
	blacklistCleanupTick <-chan time.Time
	emitStatsTick        <-chan time.Time

	announceClient announceclient.Client

	networkEventProducer networkevent.Producer
	eventLogger          *zap.SugaredLogger

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
	eventLogger *zap.SugaredLogger,
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

	var aq announceQueue
	if pctx.Origin {
		// Origin peers should not announce.
		aq = disabledAnnounceQueue{}
	} else {
		aq = newAnnounceQueue()
	}

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
			Config:               config.Dispatcher,
			LocalPeerID:          pctx.PeerID,
			EventSender:          overrides.eventLoop,
			Clock:                overrides.clock,
			NetworkEventProducer: networkEventProducer,
			EventLogger:          eventLogger,
			Stats:                stats,
		},
		torrentControls:      make(map[torlib.InfoHash]*torrentControl),
		connState:            newConnState(pctx.PeerID, config.ConnState, overrides.clock),
		announceQueue:        aq,
		eventLoop:            overrides.eventLoop,
		listener:             l,
		announceTick:         overrides.clock.Tick(config.AnnounceInterval),
		preemptionTick:       preemptionTick,
		blacklistCleanupTick: blacklistCleanupTick,
		emitStatsTick:        overrides.clock.Tick(config.EmitStatsInterval),
		announceClient:       announceClient,
		networkEventProducer: networkEventProducer,
		eventLogger:          eventLogger,
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

// Reload transfers s into a new Scheduler with new config. After reloading, s
// is unusable.
func Reload(s *Scheduler, config Config) (*Scheduler, error) {
	s.Stop()
	return New(config, s.torrentArchive, s.stats, s.pctx, s.announceClient, s.networkEventProducer, s.eventLogger)
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
			ctrl.Dispatcher.TearDown()
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
func (s *Scheduler) AddTorrent(name string) <-chan error {
	// Buffer size of 1 so sends do not block.
	errc := make(chan error, 1)

	t, err := s.torrentArchive.GetTorrent(name)
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
func (s *Scheduler) CancelTorrent(name string) {
	s.eventLoop.Send(cancelTorrentEvent{name})
}

// BlacklistedConn represents a connection which has been blacklisted.
type BlacklistedConn struct {
	PeerID    torlib.PeerID   `json:"peer_id"`
	InfoHash  torlib.InfoHash `json:"info_hash"`
	Remaining time.Duration   `json:"remaining"`
}

// BlacklistSnapshot returns a snapshot of the current connection blacklist.
func (s *Scheduler) BlacklistSnapshot() (chan []BlacklistedConn, error) {
	result := make(chan []BlacklistedConn)
	if !s.eventLoop.Send(blacklistSnapshotEvent{result}) {
		return nil, ErrSchedulerStopped
	}
	return result, nil
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
	nc net.Conn,
	remoteHandshake *handshake) (c *conn, remoteBitfield storage.Bitfield, t storage.Torrent, err error) {

	t, err = s.torrentArchive.GetTorrent(remoteHandshake.Name)
	if err != nil {
		err = fmt.Errorf("get torrent for blob %s: %s", remoteHandshake.Name, err)
		return
	}
	if t.InfoHash() != remoteHandshake.InfoHash {
		err = fmt.Errorf("info hash mismatch for blob %s", remoteHandshake.Name)
		return
	}
	c, remoteBitfield, err = s.connFactory.ReciprocateHandshake(nc, t, remoteHandshake)
	if err != nil {
		err = fmt.Errorf("reciprocate handshake for blob %s: %s", remoteHandshake.Name, err)
		return
	}
	return c, remoteBitfield, t, nil
}

func (s *Scheduler) initIncomingConn(nc net.Conn, remoteHandshake *handshake) {
	s.log("peer", remoteHandshake.PeerID).Info("Handshaking incoming connection")

	var e event
	c, bitfield, t, err := s.doInitIncomingConn(nc, remoteHandshake)
	if err != nil {
		nc.Close()
		s.log("handshake", remoteHandshake).Errorf("Error initializing incoming connection: %s", err)
		e = failedHandshakeEvent{remoteHandshake.PeerID, remoteHandshake.InfoHash}
	} else {
		e = incomingConnEvent{c, bitfield, t}
	}
	s.eventLoop.Send(e)
}

func (s *Scheduler) doInitOutgoingConn(
	peerID torlib.PeerID,
	ip string,
	port int,
	t storage.Torrent) (c *conn, remoteBitfield storage.Bitfield, err error) {

	addr := fmt.Sprintf("%s:%d", ip, port)
	nc, err := net.DialTimeout("tcp", addr, s.config.DialTimeout)
	if err != nil {
		err = fmt.Errorf("failed to dial peer: %s", err)
		return
	}
	c, remoteBitfield, err = s.connFactory.SendAndReceiveHandshake(nc, t)
	if err != nil {
		nc.Close()
		err = fmt.Errorf("failed to handshake peer: %s", err)
		return
	}
	if c.PeerID != peerID {
		c.Close()
		err = errors.New("unexpected peer id from handshaked conn")
		return
	}
	return c, remoteBitfield, nil
}

func (s *Scheduler) initOutgoingConn(peerID torlib.PeerID, ip string, port int, t storage.Torrent) {
	s.log("peer", peerID, "ip", ip, "port", port, "torrent", t).Info(
		"Initializing outgoing connection")

	var e event
	c, bitfield, err := s.doInitOutgoingConn(peerID, ip, port, t)
	if err != nil {
		s.log("peer", peerID, "ip", ip, "port", port, "torrent", t).Errorf(
			"Error intializing outgoing connection: %s", err)
		e = failedHandshakeEvent{peerID, t.InfoHash()}
	} else {
		e = outgoingConnEvent{c, bitfield, t}
	}
	s.eventLoop.Send(e)
}

func (s *Scheduler) announce(d *dispatcher) {
	var e event
	peers, err := s.announceClient.Announce(
		d.Torrent.Name(), d.Torrent.InfoHash(), d.Torrent.Complete())
	if err != nil {
		s.log("dispatcher", d).Errorf("Announce failed: %s", err)
		e = announceFailureEvent{d}
	} else {
		e = announceResponseEvent{d.Torrent.InfoHash(), peers}
	}
	s.eventLoop.Send(e)
}

func (s *Scheduler) addOutgoingConn(c *conn, b storage.Bitfield, t storage.Torrent) error {
	if err := s.connState.MovePendingToActive(c); err != nil {
		return fmt.Errorf("cannot add conn to scheduler: %s", err)
	}
	ctrl, ok := s.torrentControls[t.InfoHash()]
	if !ok {
		return errors.New("torrent must be created before sending handshake")
	}
	if err := ctrl.Dispatcher.AddPeer(c.PeerID, b, c); err != nil {
		return fmt.Errorf("cannot add conn to dispatcher: %s", err)
	}
	return nil
}

func (s *Scheduler) addIncomingConn(c *conn, b storage.Bitfield, t storage.Torrent) error {
	if err := s.connState.MovePendingToActive(c); err != nil {
		return fmt.Errorf("cannot add conn to scheduler: %s", err)
	}
	ctrl, ok := s.torrentControls[t.InfoHash()]
	if !ok {
		ctrl = s.initTorrentControl(t)
	}
	if err := ctrl.Dispatcher.AddPeer(c.PeerID, b, c); err != nil {
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
	s.eventLogger.Info(networkevent.AddTorrentEvent(t.InfoHash(), s.pctx.PeerID, t.Bitfield()).JSON())
	s.torrentControls[t.InfoHash()] = ctrl
	return ctrl
}

func (s *Scheduler) log(args ...interface{}) *zap.SugaredLogger {
	return log.With(args...)
}
