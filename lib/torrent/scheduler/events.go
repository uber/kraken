package scheduler

import (
	"fmt"
	"time"

	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/timeutil"

	"github.com/willf/bitset"
)

// event describes an external event which moves the Scheduler into a new state.
// While the event is applying, it is guaranteed to be the only accessor of
// Scheduler state.
type event interface {
	Apply(s *Scheduler)
}

// eventSender is a subset of the eventLoop which can only send events.
type eventSender interface {
	Send(event) bool
}

// eventLoop represents a serialized list of events to be applied to a Scheduler.
type eventLoop interface {
	eventSender
	Run(*Scheduler)
	Stop()
}

type eventLoopImpl struct {
	events chan event
	done   chan struct{}
}

func newEventLoop() *eventLoopImpl {
	return &eventLoopImpl{
		events: make(chan event),
		done:   make(chan struct{}),
	}
}

// Send sends a new event into l. Should never be called by the same goroutine
// running l (i.e. within Apply methods), else deadlock will occur. Returns false
// if the l is not running.
func (l *eventLoopImpl) Send(e event) bool {
	select {
	case l.events <- e:
		return true
	case <-l.done:
		return false
	}
}

// Run processes events until done is closed.
func (l *eventLoopImpl) Run(s *Scheduler) {
	for {
		select {
		case e := <-l.events:
			e.Apply(s)
		case <-l.done:
			return
		}
	}
}

func (l *eventLoopImpl) Stop() {
	close(l.done)
}

// closedConnEvent occurs when a connection is closed.
type closedConnEvent struct {
	c *conn.Conn
}

// Apply ejects the conn from the Scheduler's active connections.
func (e closedConnEvent) Apply(s *Scheduler) {
	s.connState.DeleteActive(e.c)
	if err := s.connState.Blacklist(e.c.PeerID(), e.c.InfoHash()); err != nil {
		s.log("conn", e.c).Infof("Cannot blacklist active conn: %s", err)
	}
}

// incomingHandshakeEvent when a handshake was received from a new connection.
type incomingHandshakeEvent struct {
	pc *conn.PendingConn
}

// Apply rejects incoming handshakes when the Scheduler is at capacity. If the
// Scheduler has capacity for more connections, adds the peer/hash of the handshake
// to the Scheduler's pending connections and asynchronously attempts to establish
// the connection.
func (e incomingHandshakeEvent) Apply(s *Scheduler) {
	if err := s.connState.AddPending(e.pc.PeerID(), e.pc.InfoHash()); err != nil {
		s.log("peer", e.pc.PeerID(), "hash", e.pc.InfoHash()).Infof(
			"Rejecting incoming handshake: %s", err)
		e.pc.Close()
		return
	}
	go func() {
		info, err := s.torrentArchive.Stat(e.pc.Name())
		if err != nil {
			e.pc.Close()
			return
		}
		c, err := s.handshaker.Establish(e.pc, info)
		if err != nil {
			s.log("peer", e.pc.PeerID(), "hash", e.pc.InfoHash()).Infof(
				"Error establishing conn: %s", err)
			e.pc.Close()
			s.eventLoop.Send(failedIncomingHandshakeEvent{e.pc.PeerID(), e.pc.InfoHash()})
			return
		}
		s.eventLoop.Send(incomingConnEvent{c, e.pc.Bitfield(), info})
	}()
}

// failedIncomingHandshakeEvent occurs when a pending incoming connection fails
// to handshake.
type failedIncomingHandshakeEvent struct {
	peerID   torlib.PeerID
	infoHash torlib.InfoHash
}

func (e failedIncomingHandshakeEvent) Apply(s *Scheduler) {
	s.connState.DeletePending(e.peerID, e.infoHash)
}

// incomingConnEvent occurs when a pending incoming connection finishes handshaking.
type incomingConnEvent struct {
	c        *conn.Conn
	bitfield *bitset.BitSet
	info     *storage.TorrentInfo
}

// Apply transitions a fully-handshaked incoming conn from pending to active.
func (e incomingConnEvent) Apply(s *Scheduler) {
	if err := s.addIncomingConn(e.c, e.bitfield, e.info); err != nil {
		s.log("conn", e.c).Errorf("Error adding incoming conn: %s", err)
		e.c.Close()
		return
	}
	s.log("conn", e.c).Infof("Added incoming conn with %d%% downloaded", e.info.PercentDownloaded())
}

// failedOutgoingHandshakeEvent occurs when a pending incoming connection fails
// to handshake.
type failedOutgoingHandshakeEvent struct {
	peerID   torlib.PeerID
	infoHash torlib.InfoHash
}

func (e failedOutgoingHandshakeEvent) Apply(s *Scheduler) {
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
func (e outgoingConnEvent) Apply(s *Scheduler) {
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
func (e announceTickEvent) Apply(s *Scheduler) {
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
	s.log("dispatcher", ctrl.Dispatcher).Debug("Announcing")
	go s.announce(ctrl.Dispatcher)
}

// announceResponseEvent occurs when a successfully announce response was received
// from the tracker.
type announceResponseEvent struct {
	infoHash torlib.InfoHash
	peers    []torlib.PeerInfo
}

// Apply selects new peers returned via an announce response to open connections to
// if there is capacity. These connections are added to the Scheduler's pending
// connections and handshaked asynchronously.
//
// Also marks the dispatcher as ready to announce again.
func (e announceResponseEvent) Apply(s *Scheduler) {
	ctrl, ok := s.torrentControls[e.infoHash]
	if !ok {
		s.log("hash", e.infoHash).Info("Dispatcher closed after announce response received")
		return
	}
	s.announceQueue.Ready(e.infoHash)
	if ctrl.Complete {
		// Torrent is already complete, don't open any new connections.
		return
	}
	for i := 0; i < len(e.peers); i++ {
		p := e.peers[i]
		peerID, err := torlib.NewPeerID(p.PeerID)
		if err != nil {
			s.log("peer", p.PeerID, "hash", e.infoHash).Errorf(
				"Error creating PeerID from announce response: %s", err)
			continue
		}
		if peerID == s.pctx.PeerID {
			// Tracker may return our own peer.
			continue
		}
		if s.connState.Blacklisted(peerID, e.infoHash) {
			continue
		}
		if err := s.connState.AddPending(peerID, e.infoHash); err != nil {
			if err == errTorrentAtCapacity {
				break
			}
			continue
		}
		go func() {
			addr := fmt.Sprintf("%s:%d", p.IP, int(p.Port))
			info := ctrl.Dispatcher.Torrent.Stat()
			c, bitfield, err := s.handshaker.Initialize(peerID, addr, info)
			if err != nil {
				s.log("peer", peerID, "hash", e.infoHash, "addr", addr).Infof(
					"Failed handshake: %s", err)
				s.eventLoop.Send(failedOutgoingHandshakeEvent{peerID, e.infoHash})
				return
			}
			s.eventLoop.Send(outgoingConnEvent{c, bitfield, info})
		}()
	}
}

// announceFailureEvent occurs when an announce request fails.
type announceFailureEvent struct {
	infoHash torlib.InfoHash
}

// Apply marks the dispatcher as ready to announce again.
func (e announceFailureEvent) Apply(s *Scheduler) {
	s.announceQueue.Ready(e.infoHash)
}

// newTorrentEvent occurs when a new torrent was requested for download.
type newTorrentEvent struct {
	torrent storage.Torrent
	errc    chan error
}

// Apply begins seeding / leeching a new torrent.
func (e newTorrentEvent) Apply(s *Scheduler) {
	ctrl, ok := s.torrentControls[e.torrent.InfoHash()]
	if !ok {
		ctrl = s.initTorrentControl(e.torrent, true)
		s.log("torrent", e.torrent).Info("Initialized new torrent")
	}
	if ctrl.Complete {
		e.errc <- nil
		return
	}
	ctrl.Errors = append(ctrl.Errors, e.errc)
}

// completedDispatcherEvent occurs when a dispatcher finishes downloading its torrent.
type completedDispatcherEvent struct {
	dispatcher *dispatcher
}

// Apply marks the dispatcher for its final announce.
func (e completedDispatcherEvent) Apply(s *Scheduler) {
	infoHash := e.dispatcher.Torrent.InfoHash()

	s.connState.ClearBlacklist(infoHash)
	s.announceQueue.Done(infoHash)
	ctrl, ok := s.torrentControls[infoHash]
	if !ok {
		s.log("dispatcher", e.dispatcher).Error("Completed dispatcher not found")
		return
	}
	for _, errc := range ctrl.Errors {
		errc <- nil
	}
	ctrl.Complete = true
	if ctrl.LocalRequest {
		// Normalize the download time for all torrent sizes to a per KB value.
		// Skip torrents that are less than a KB in size because we can't measure
		// at that granularity.
		downloadTime := ctrl.Dispatcher.CreatedAt.Sub(s.clock.Now())
		lengthKB := ctrl.Dispatcher.Torrent.Length() / int64(memsize.KB)
		if lengthKB > 0 {
			s.stats.Timer("download_time_per_kb").Record(downloadTime / time.Duration(lengthKB))
		}
	}

	s.log("torrent", e.dispatcher.Torrent).Info("Torrent complete")
	s.networkEvents.Produce(networkevent.TorrentCompleteEvent(infoHash, s.pctx.PeerID))
}

// preemptionTickEvent occurs periodically to preempt unneeded conns and remove
// idle torrentControls.
type preemptionTickEvent struct{}

func (e preemptionTickEvent) Apply(s *Scheduler) {
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
			ctrl.Dispatcher.LastGoodPieceReceived(c.PeerID()),
			ctrl.Dispatcher.LastPieceSent(c.PeerID()))
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
		if ctrl.Complete {
			if s.clock.Now().Sub(ctrl.Dispatcher.LastReadTime()) >= s.config.SeederTTI {
				s.log("hash", infoHash).Info("Removing idle seeding torrent")
				delete(s.torrentControls, infoHash)
			}
		} else {
			if s.clock.Now().Sub(ctrl.Dispatcher.LastWriteTime()) >= s.config.LeecherTTI {
				s.log("hash", infoHash).Info("Cancelling idle in-progress torrent")
				ctrl.Dispatcher.TearDown()
				s.announceQueue.Eject(infoHash)
				for _, errc := range ctrl.Errors {
					errc <- ErrTorrentTimeout
				}
				delete(s.torrentControls, infoHash)
				s.networkEvents.Produce(networkevent.TorrentCancelledEvent(infoHash, s.pctx.PeerID))
			}
		}
	}
}

// emitStatsEvent occurs periodically to emit Scheduler stats.
type emitStatsEvent struct{}

func (e emitStatsEvent) Apply(s *Scheduler) {
	s.stats.Gauge("torrents").Update(float64(len(s.torrentControls)))
	s.stats.Gauge("conns").Update(float64(s.connState.NumActiveConns()))
}

type blacklistSnapshotEvent struct {
	result chan []BlacklistedConn
}

func (e blacklistSnapshotEvent) Apply(s *Scheduler) {
	e.result <- s.connState.BlacklistSnapshot()
}
