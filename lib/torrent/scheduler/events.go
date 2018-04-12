package scheduler

import (
	"fmt"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/connstate"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/dispatch"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/timeutil"

	"github.com/willf/bitset"
)

// event describes an external event which moves the scheduler into a new state.
// While the event is applying, it is guaranteed to be the only accessor of
// scheduler state.
type event interface {
	Apply(s *scheduler)
}

// eventLoop represents a serialized list of events to be applied to a scheduler.
type eventLoop interface {
	Send(event) bool
	SendTimeout(e event, timeout time.Duration) error
	Run(*scheduler)
	Stop()
}

type defaultEventLoop struct {
	events chan event
	done   chan struct{}
}

func newEventLoop() *defaultEventLoop {
	return &defaultEventLoop{
		events: make(chan event),
		done:   make(chan struct{}),
	}
}

// Send sends a new event into l. Should never be called by the same goroutine
// running l (i.e. within Apply methods), else deadlock will occur. Returns false
// if the l is not running.
func (l *defaultEventLoop) Send(e event) bool {
	select {
	case l.events <- e:
		return true
	case <-l.done:
		return false
	}
}

func (l *defaultEventLoop) SendTimeout(e event, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case l.events <- e:
		return nil
	case <-l.done:
		return ErrSchedulerStopped
	case <-timer.C:
		return ErrSendEventTimedOut
	}
}

// Run processes events until done is closed.
func (l *defaultEventLoop) Run(s *scheduler) {
	for {
		select {
		case e := <-l.events:
			e.Apply(s)
		case <-l.done:
			return
		}
	}
}

func (l *defaultEventLoop) Stop() {
	close(l.done)
}

type liftedEventLoop struct {
	eventLoop
}

// liftEventLoop lifts events from subpackages into an eventLoop.
func liftEventLoop(l eventLoop) *liftedEventLoop {
	return &liftedEventLoop{l}
}

func (l *liftedEventLoop) ConnClosed(c *conn.Conn) {
	l.Send(connClosedEvent{c})
}

func (l *liftedEventLoop) DispatcherComplete(d *dispatch.Dispatcher) {
	l.Send(dispatcherCompleteEvent{d})
}

func (l *liftedEventLoop) AnnounceTick() {
	l.Send(announceTickEvent{})
}

// connClosedEvent occurs when a connection is closed.
type connClosedEvent struct {
	c *conn.Conn
}

// Apply ejects the conn from the scheduler's active connections.
func (e connClosedEvent) Apply(s *scheduler) {
	s.connState.DeleteActive(e.c)
	if err := s.connState.Blacklist(e.c.PeerID(), e.c.InfoHash()); err != nil {
		s.log("conn", e.c).Infof("Cannot blacklist active conn: %s", err)
	}
}

// incomingHandshakeEvent when a handshake was received from a new connection.
type incomingHandshakeEvent struct {
	pc *conn.PendingConn
}

// Apply rejects incoming handshakes when the scheduler is at capacity. If the
// scheduler has capacity for more connections, adds the peer/hash of the handshake
// to the scheduler's pending connections and asynchronously attempts to establish
// the connection.
func (e incomingHandshakeEvent) Apply(s *scheduler) {
	if err := s.connState.AddPending(e.pc.PeerID(), e.pc.InfoHash()); err != nil {
		s.log("peer", e.pc.PeerID(), "hash", e.pc.InfoHash()).Infof(
			"Rejecting incoming handshake: %s", err)
		e.pc.Close()
		return
	}
	go func() {
		fail := func(err error) {
			s.log("peer", e.pc.PeerID(), "hash", e.pc.InfoHash()).Infof(
				"Error processing incoming handshake: %s", err)
			e.pc.Close()
			s.eventLoop.Send(failedIncomingHandshakeEvent{e.pc.PeerID(), e.pc.InfoHash()})
		}
		info, err := s.torrentArchive.Stat(e.pc.Namespace(), e.pc.Name())
		if err != nil {
			fail(fmt.Errorf("torrent stat: %s", err))
			return
		}
		c, err := s.handshaker.Establish(e.pc, info)
		if err != nil {
			fail(fmt.Errorf("establish handshake: %s", err))
			return
		}
		s.eventLoop.Send(incomingConnEvent{e.pc.Namespace(), c, e.pc.Bitfield(), info})
	}()
}

// failedIncomingHandshakeEvent occurs when a pending incoming connection fails
// to handshake.
type failedIncomingHandshakeEvent struct {
	peerID   core.PeerID
	infoHash core.InfoHash
}

func (e failedIncomingHandshakeEvent) Apply(s *scheduler) {
	s.connState.DeletePending(e.peerID, e.infoHash)
}

// incomingConnEvent occurs when a pending incoming connection finishes handshaking.
type incomingConnEvent struct {
	namespace string
	c         *conn.Conn
	bitfield  *bitset.BitSet
	info      *storage.TorrentInfo
}

// Apply transitions a fully-handshaked incoming conn from pending to active.
func (e incomingConnEvent) Apply(s *scheduler) {
	if err := s.addIncomingConn(e.namespace, e.c, e.bitfield, e.info); err != nil {
		s.log("conn", e.c).Errorf("Error adding incoming conn: %s", err)
		e.c.Close()
		return
	}
	s.log("conn", e.c).Info("Added incoming conn")
}

// failedOutgoingHandshakeEvent occurs when a pending incoming connection fails
// to handshake.
type failedOutgoingHandshakeEvent struct {
	peerID   core.PeerID
	infoHash core.InfoHash
}

func (e failedOutgoingHandshakeEvent) Apply(s *scheduler) {
	s.connState.DeletePending(e.peerID, e.infoHash)
	if err := s.connState.Blacklist(e.peerID, e.infoHash); err != nil {
		s.log("peer", e.peerID, "hash", e.infoHash).Infof("Cannot blacklist pending conn: %s", err)
	}
}

// outgoingConnEvent occurs when a pending outgoing connection finishes handshaking.
type outgoingConnEvent struct {
	c        *conn.Conn
	bitfield *bitset.BitSet
	info     *storage.TorrentInfo
}

// Apply transitions a fully-handshaked outgoing conn from pending to active.
func (e outgoingConnEvent) Apply(s *scheduler) {
	if err := s.addOutgoingConn(e.c, e.bitfield, e.info); err != nil {
		s.log("conn", e.c).Errorf("Error adding outgoing conn: %s", err)
		e.c.Close()
		return
	}
	s.log("conn", e.c).Infof("Added outgoing conn with %d%% downloaded", e.info.PercentDownloaded())
}

// announceTickEvent occurs when it is time to announce to the tracker.
type announceTickEvent struct{}

// Apply pulls the next dispatcher from the announce queue and asynchronously
// makes an announce request to the tracker.
func (e announceTickEvent) Apply(s *scheduler) {
	h, ok := s.announceQueue.Next()
	if !ok {
		s.log().Debug("No torrents in announce queue")
		return
	}
	ctrl, ok := s.torrentControls[h]
	if !ok {
		s.log("hash", h).Error("Pulled unknown torrent off announce queue")
		return
	}
	if ctrl.dispatcher.NumPeers() >= s.connState.MaxConnsPerTorrent() {
		s.log("hash", h).Info("Skipping announce for fully saturated torrent")
		return
	}
	go s.announce(ctrl.dispatcher)
}

// announceResultEvent occurs when a successfully announce response was received
// from the tracker.
type announceResultEvent struct {
	infoHash core.InfoHash
	peers    []*core.PeerInfo
}

// Apply selects new peers returned via an announce response to open connections to
// if there is capacity. These connections are added to the scheduler's pending
// connections and handshaked asynchronously.
//
// Also marks the dispatcher as ready to announce again.
func (e announceResultEvent) Apply(s *scheduler) {
	ctrl, ok := s.torrentControls[e.infoHash]
	if !ok {
		s.log("hash", e.infoHash).Info("Dispatcher closed after announce response received")
		return
	}
	s.announceQueue.Ready(e.infoHash)
	if ctrl.complete {
		// Torrent is already complete, don't open any new connections.
		return
	}
	for _, p := range e.peers {
		if p.PeerID == s.pctx.PeerID {
			// Tracker may return our own peer.
			continue
		}
		if s.connState.Blacklisted(p.PeerID, e.infoHash) {
			continue
		}
		if err := s.connState.AddPending(p.PeerID, e.infoHash); err != nil {
			if err == connstate.ErrTorrentAtCapacity {
				break
			}
			continue
		}
		go func(p *core.PeerInfo) {
			addr := fmt.Sprintf("%s:%d", p.IP, p.Port)
			info := ctrl.dispatcher.Stat()
			c, bitfield, err := s.handshaker.Initialize(p.PeerID, addr, info, ctrl.namespace)
			if err != nil {
				s.log("peer", p.PeerID, "hash", e.infoHash, "addr", addr).Infof(
					"Failed handshake: %s", err)
				s.eventLoop.Send(failedOutgoingHandshakeEvent{p.PeerID, e.infoHash})
				return
			}
			s.eventLoop.Send(outgoingConnEvent{c, bitfield, info})
		}(p)
	}
}

// announceErrEvent occurs when an announce request fails.
type announceErrEvent struct {
	infoHash core.InfoHash
	err      error
}

// Apply marks the dispatcher as ready to announce again.
func (e announceErrEvent) Apply(s *scheduler) {
	s.log("hash", e.infoHash).Errorf("Error announcing: %s", e.err)
	s.announceQueue.Ready(e.infoHash)
}

// newTorrentEvent occurs when a new torrent was requested for download.
type newTorrentEvent struct {
	namespace string
	torrent   storage.Torrent
	errc      chan error
}

// Apply begins seeding / leeching a new torrent.
func (e newTorrentEvent) Apply(s *scheduler) {
	ctrl, ok := s.torrentControls[e.torrent.InfoHash()]
	if !ok {
		ctrl = s.initTorrentControl(e.namespace, e.torrent, true)
		s.log("torrent", e.torrent).Info("Added new torrent")
	}
	if ctrl.complete {
		e.errc <- nil
		return
	}
	ctrl.errors = append(ctrl.errors, e.errc)

	// Immediately announce new torrents.
	go s.announce(ctrl.dispatcher)
}

// dispatcherCompleteEvent occurs when a dispatcher finishes downloading its torrent.
type dispatcherCompleteEvent struct {
	dispatcher *dispatch.Dispatcher
}

// Apply marks the dispatcher for its final announce.
func (e dispatcherCompleteEvent) Apply(s *scheduler) {
	infoHash := e.dispatcher.InfoHash()

	s.connState.ClearBlacklist(infoHash)
	s.announceQueue.Eject(infoHash)
	ctrl, ok := s.torrentControls[infoHash]
	if !ok {
		s.log("dispatcher", e.dispatcher).Error("Completed dispatcher not found")
		return
	}
	for _, errc := range ctrl.errors {
		errc <- nil
	}
	ctrl.complete = true
	if ctrl.localRequest {
		// Normalize the download time for all torrent sizes to a per MB value.
		// Skip torrents that are less than a MB in size because we can't measure
		// at that granularity.
		downloadTime := s.clock.Now().Sub(ctrl.dispatcher.CreatedAt())
		lengthMB := ctrl.dispatcher.Length() / int64(memsize.MB)
		if lengthMB > 0 {
			s.stats.Timer("download_time_per_mb").Record(downloadTime / time.Duration(lengthMB))
		}
	}

	s.log("hash", infoHash).Info("Torrent complete")
	s.networkEvents.Produce(networkevent.TorrentCompleteEvent(infoHash, s.pctx.PeerID))

	// Immediately announce completed torrents.
	go s.announce(ctrl.dispatcher)
}

// preemptionTickEvent occurs periodically to preempt unneeded conns and remove
// idle torrentControls.
type preemptionTickEvent struct{}

func (e preemptionTickEvent) Apply(s *scheduler) {
	for _, c := range s.connState.ActiveConns() {
		ctrl, ok := s.torrentControls[c.InfoHash()]
		if !ok {
			s.log("conn", c).Error(
				"Invariant violation: active conn not assigned to dispatcher")
			c.Close()
			continue
		}
		lastProgress := timeutil.MostRecent(
			c.CreatedAt(),
			ctrl.dispatcher.LastGoodPieceReceived(c.PeerID()),
			ctrl.dispatcher.LastPieceSent(c.PeerID()))
		if s.clock.Now().Sub(lastProgress) > s.config.ConnTTI {
			s.log("conn", c).Info("Closing idle conn")
			c.Close()
			continue
		}
		if s.clock.Now().Sub(c.CreatedAt()) > s.config.ConnTTL {
			s.log("conn", c).Info("Closing expired conn")
			c.Close()
			continue
		}
	}

	for infoHash, ctrl := range s.torrentControls {
		idleSeeder :=
			ctrl.complete &&
				s.clock.Now().Sub(ctrl.dispatcher.LastReadTime()) >= s.config.SeederTTI
		idleLeecher :=
			!ctrl.complete &&
				s.clock.Now().Sub(ctrl.dispatcher.LastWriteTime()) >= s.config.LeecherTTI
		if idleSeeder || idleLeecher {
			s.log("hash", infoHash, "inprogress", !ctrl.complete).Info("Removing idle torrent")
			s.tearDownTorrentControl(ctrl, ErrTorrentTimeout)
		}
	}
}

// emitStatsEvent occurs periodically to emit scheduler stats.
type emitStatsEvent struct{}

func (e emitStatsEvent) Apply(s *scheduler) {
	s.stats.Gauge("torrents").Update(float64(len(s.torrentControls)))
	s.stats.Gauge("conns").Update(float64(s.connState.NumActiveConns()))
}

type blacklistSnapshotEvent struct {
	result chan []connstate.BlacklistedConn
}

func (e blacklistSnapshotEvent) Apply(s *scheduler) {
	e.result <- s.connState.BlacklistSnapshot()
}

// removeTorrentEvent occurs when a torrent is manually removed via scheduler API.
type removeTorrentEvent struct {
	name string
	errc chan error
}

func (e removeTorrentEvent) Apply(s *scheduler) {
	for _, ctrl := range s.torrentControls {
		if ctrl.dispatcher.Name() == e.name {
			s.log(
				"hash", ctrl.dispatcher.InfoHash(),
				"inprogress", !ctrl.complete).Info("Removing torrent")
			s.tearDownTorrentControl(ctrl, ErrTorrentRemoved)
		}
	}
	e.errc <- s.torrentArchive.DeleteTorrent(e.name)
}

// probeEvent occurs when a probe is manually requested via scheduler API.
// The event loop is unbuffered, so if a probe can be successfully sent, then
// the event loop is healthy.
type probeEvent struct{}

func (e probeEvent) Apply(*scheduler) {}
