package scheduler

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
	"github.com/willf/bitset"
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/announcequeue"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/connstate"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/dispatch"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"
)

// Scheduler errors.
var (
	ErrTorrentNotFound   = errors.New("torrent not found")
	ErrSchedulerStopped  = errors.New("scheduler has been stopped")
	ErrTorrentTimeout    = errors.New("torrent timed out")
	ErrTorrentRemoved    = errors.New("torrent manually removed")
	ErrSendEventTimedOut = errors.New("event loop send timed out")
)

// Scheduler defines operations for scheduler.
type Scheduler interface {
	Stop()
	Download(namespace, name string) error
	BlacklistSnapshot() ([]connstate.BlacklistedConn, error)
	RemoveTorrent(name string) error
	Probe() error
}

// torrentControl bundles torrent control structures.
type torrentControl struct {
	errors       []chan error
	dispatcher   *dispatch.Dispatcher
	complete     bool
	localRequest bool
}

func newTorrentControl(d *dispatch.Dispatcher, localRequest bool) *torrentControl {
	return &torrentControl{
		dispatcher:   d,
		localRequest: localRequest,
	}
}

// scheduler manages global state for the peer. This includes:
// - Opening torrents.
// - Announcing to the tracker.
// - Handshaking incoming connections.
// - Initializing outgoing connections.
// - Dispatching connections to torrents.
// - Pre-empting existing connections when better options are available (TODO).
type scheduler struct {
	pctx           core.PeerContext
	config         Config
	clock          clock.Clock
	torrentArchive storage.TorrentArchive
	stats          tally.Scope

	handshaker *conn.Handshaker

	// The following fields define the core scheduler "state", and should only
	// be accessed from within the event loop.
	torrentControls map[core.InfoHash]*torrentControl // Active seeding / leeching torrents.
	connState       *connstate.State
	announceQueue   announcequeue.Queue

	eventLoop *liftedEventLoop

	listener net.Listener

	announceTick   <-chan time.Time
	preemptionTick <-chan time.Time
	emitStatsTick  <-chan time.Time

	announceClient announceclient.Client

	networkEvents networkevent.Producer

	// The following fields orchestrate the stopping of the scheduler.
	stopOnce sync.Once      // Ensures the stop sequence is executed only once.
	done     chan struct{}  // Signals all goroutines to exit.
	wg       sync.WaitGroup // Waits for eventLoop and listenLoop to exit.
}

// schedOverrides defines scheduler fields which may be overrided for testing
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

// newScheduler creates and starts a scheduler.
func newScheduler(
	config Config,
	ta storage.TorrentArchive,
	stats tally.Scope,
	pctx core.PeerContext,
	announceClient announceclient.Client,
	announceQueue announcequeue.Queue,
	networkEvents networkevent.Producer,
	options ...option) (*scheduler, error) {

	config = config.applyDefaults()

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", pctx.Port))
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	stats = stats.Tagged(map[string]string{
		"module": "scheduler",
	})

	overrides := schedOverrides{
		clock:     clock.New(),
		eventLoop: newEventLoop(),
	}
	for _, opt := range options {
		opt(&overrides)
	}

	eventLoop := liftEventLoop(overrides.eventLoop)

	var preemptionTick <-chan time.Time
	if !config.DisablePreemption {
		preemptionTick = overrides.clock.Tick(config.PreemptionInterval)
	}

	handshaker := conn.NewHandshaker(
		config.Conn, stats, overrides.clock, networkEvents, pctx.PeerID, eventLoop)

	connState := connstate.New(config.ConnState, overrides.clock, pctx.PeerID, networkEvents)

	s := &scheduler{
		pctx:            pctx,
		config:          config,
		clock:           overrides.clock,
		torrentArchive:  ta,
		stats:           stats,
		handshaker:      handshaker,
		torrentControls: make(map[core.InfoHash]*torrentControl),
		connState:       connState,
		announceQueue:   announceQueue,
		eventLoop:       eventLoop,
		listener:        l,
		announceTick:    overrides.clock.Tick(config.AnnounceInterval),
		preemptionTick:  preemptionTick,
		emitStatsTick:   overrides.clock.Tick(config.EmitStatsInterval),
		announceClient:  announceClient,
		networkEvents:   networkEvents,
		done:            done,
	}

	if config.DisablePreemption {
		s.log().Warn("Preemption disabled")
	}
	if config.ConnState.DisableBlacklist {
		s.log().Warn("Blacklisting disabled")
	}

	log.Infof("Scheduler starting as peer %s on addr %s:%d", pctx.PeerID, pctx.IP, pctx.Port)

	s.start()

	return s, nil
}

// Stop shuts down the scheduler.
func (s *scheduler) Stop() {
	s.stopOnce.Do(func() {
		s.log().Info("Stopping scheduler...")

		close(s.done)
		s.listener.Close()
		s.eventLoop.Stop()

		// Waits for all loops to stop.
		s.wg.Wait()

		for _, c := range s.connState.ActiveConns() {
			s.log("conn", c).Info("Closing conn to stop scheduler")
			c.Close()
		}

		// Notify local clients of pending torrents that they will not complete.
		for _, ctrl := range s.torrentControls {
			ctrl.dispatcher.TearDown()
			for _, errc := range ctrl.errors {
				errc <- ErrSchedulerStopped
			}
		}

		s.log().Info("Scheduler stopped")
	})
}

func (s *scheduler) doDownload(namespace, name string) (size int64, err error) {
	t, err := s.torrentArchive.CreateTorrent(namespace, name)
	if err != nil {
		if err == storage.ErrNotFound {
			return 0, ErrTorrentNotFound
		}
		return 0, fmt.Errorf("create torrent: %s", err)
	}

	// Buffer size of 1 so sends do not block.
	errc := make(chan error, 1)
	if !s.eventLoop.Send(newTorrentEvent{t, errc}) {
		return 0, ErrSchedulerStopped
	}
	return t.Length(), <-errc
}

// Download downloads the torrent given metainfo. Once the torrent is downloaded,
// it will begin seeding asynchronously.
func (s *scheduler) Download(namespace, name string) error {
	start := time.Now()
	size, err := s.doDownload(namespace, name)
	if err != nil {
		var errTag string
		switch err {
		case ErrTorrentNotFound:
			errTag = "not_found"
		case ErrTorrentTimeout:
			errTag = "timeout"
		case ErrSchedulerStopped:
			errTag = "scheduler_stopped"
		case ErrTorrentRemoved:
			errTag = "removed"
		default:
			errTag = "unknown"
		}
		s.stats.Tagged(map[string]string{
			"error": errTag,
		}).Counter("download_errors").Inc(1)
	} else {
		s.stats.Tagged(map[string]string{
			"size": memsize.Format(getBucket(uint64(size))),
		}).Timer("download_time").Record(time.Since(start))
	}
	return err
}

// BlacklistSnapshot returns a snapshot of the current connection blacklist.
func (s *scheduler) BlacklistSnapshot() ([]connstate.BlacklistedConn, error) {
	result := make(chan []connstate.BlacklistedConn)
	if !s.eventLoop.Send(blacklistSnapshotEvent{result}) {
		return nil, ErrSchedulerStopped
	}
	return <-result, nil
}

// RemoveTorrent forcibly stops leeching / seeding torrent for name and removes
// the torrent from disk.
func (s *scheduler) RemoveTorrent(name string) error {
	// Buffer size of 1 so sends do not block.
	errc := make(chan error, 1)
	if !s.eventLoop.Send(removeTorrentEvent{name, errc}) {
		return ErrSchedulerStopped
	}
	return <-errc
}

// Probe verifies that the scheduler event loop is running and unblocked.
func (s *scheduler) Probe() error {
	return s.eventLoop.SendTimeout(probeEvent{}, s.config.ProbeTimeout)
}

func (s *scheduler) start() {
	s.wg.Add(3)
	go s.runEventLoop()
	go s.listenLoop()
	go s.tickerLoop()
}

// eventLoop handles eventLoop from the various channels of scheduler, providing synchronization to
// all scheduler state.
func (s *scheduler) runEventLoop() {
	defer s.wg.Done()

	s.eventLoop.Run(s)
}

// listenLoop accepts incoming connections.
func (s *scheduler) listenLoop() {
	defer s.wg.Done()

	s.log().Infof("Listening on %s", s.listener.Addr().String())
	for {
		nc, err := s.listener.Accept()
		if err != nil {
			// TODO Need some way to make this gracefully exit.
			s.log().Infof("Error accepting new conn, exiting listen loop: %s", err)
			return
		}
		go func() {
			pc, err := s.handshaker.Accept(nc)
			if err != nil {
				s.log().Infof("Error accepting handshake, closing net conn: %s", err)
				nc.Close()
				return
			}
			s.eventLoop.Send(incomingHandshakeEvent{pc})
		}()
	}
}

// tickerLoop periodically emits various tick events.
func (s *scheduler) tickerLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.announceTick:
			s.eventLoop.Send(announceTickEvent{})
		case <-s.preemptionTick:
			s.eventLoop.Send(preemptionTickEvent{})
		case <-s.emitStatsTick:
			s.eventLoop.Send(emitStatsEvent{})
		case <-s.done:
			return
		}
	}
}

func (s *scheduler) announce(d *dispatch.Dispatcher) {
	var e event
	peers, err := s.announceClient.Announce(
		d.Name(), d.InfoHash(), d.Complete())
	if err != nil {
		s.log("dispatcher", d).Errorf("Announce failed: %s", err)
		e = announceFailureEvent{d.InfoHash()}
	} else {
		e = announceResponseEvent{d.InfoHash(), peers}
	}
	s.eventLoop.Send(e)
}

func (s *scheduler) addOutgoingConn(c *conn.Conn, b *bitset.BitSet, info *storage.TorrentInfo) error {
	if err := s.connState.MovePendingToActive(c); err != nil {
		return fmt.Errorf("cannot add conn to scheduler: %s", err)
	}
	ctrl, ok := s.torrentControls[info.InfoHash()]
	if !ok {
		return errors.New("torrent must be created before sending handshake")
	}
	if err := ctrl.dispatcher.AddPeer(c.PeerID(), b, c); err != nil {
		return fmt.Errorf("cannot add conn to dispatcher: %s", err)
	}
	return nil
}

func (s *scheduler) addIncomingConn(c *conn.Conn, b *bitset.BitSet, info *storage.TorrentInfo) error {
	if err := s.connState.MovePendingToActive(c); err != nil {
		return fmt.Errorf("cannot add conn to scheduler: %s", err)
	}
	ctrl, ok := s.torrentControls[info.InfoHash()]
	if !ok {
		t, err := s.torrentArchive.GetTorrent(info.Name())
		if err != nil {
			return fmt.Errorf("get torrent: %s", err)
		}
		ctrl = s.initTorrentControl(t, false)
	}
	if err := ctrl.dispatcher.AddPeer(c.PeerID(), b, c); err != nil {
		return fmt.Errorf("cannot add conn to dispatcher: %s", err)
	}
	return nil
}

// initTorrentControl initializes a new torrentControl for t. Overwrites any
// existing torrentControl for t, so callers should check if one exists first.
func (s *scheduler) initTorrentControl(t storage.Torrent, localRequest bool) *torrentControl {
	d := dispatch.New(
		s.config.Dispatch,
		s.stats,
		s.clock,
		s.networkEvents,
		s.eventLoop,
		s.pctx.PeerID,
		t)
	ctrl := newTorrentControl(d, localRequest)
	s.announceQueue.Add(t.InfoHash())
	s.networkEvents.Produce(networkevent.AddTorrentEvent(
		t.InfoHash(), s.pctx.PeerID, t.Bitfield(), s.config.ConnState.MaxOpenConnectionsPerTorrent))
	s.torrentControls[t.InfoHash()] = ctrl
	return ctrl
}

func (s *scheduler) tearDownTorrentControl(ctrl *torrentControl, err error) {
	h := ctrl.dispatcher.InfoHash()
	if !ctrl.complete {
		ctrl.dispatcher.TearDown()
		s.announceQueue.Eject(h)
		for _, errc := range ctrl.errors {
			errc <- err
		}
		s.networkEvents.Produce(networkevent.TorrentCancelledEvent(h, s.pctx.PeerID))
		s.torrentArchive.DeleteTorrent(ctrl.dispatcher.Name())
	}
	delete(s.torrentControls, h)
}

func (s *scheduler) log(args ...interface{}) *zap.SugaredLogger {
	return log.With(args...)
}
