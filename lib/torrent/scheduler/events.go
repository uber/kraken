package scheduler

import (
	"net"

	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/timeutil"
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
	conn *conn
}

// Apply ejects the conn from the Scheduler's active connections.
func (e closedConnEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"conn": e.conn}).Debug("Applying closed conn event")

	if s.connState.DeleteActive(e.conn) {
		s.networkEventProducer.Produce(
			networkevent.DropConnEvent(e.conn.InfoHash, s.pctx.PeerID, e.conn.PeerID))
	}
	if err := s.connState.Blacklist(e.conn.PeerID, e.conn.InfoHash); err != nil {
		s.logf(log.Fields{"conn": e.conn}).Infof("Error blacklisting active conn: %s", err)
	}
}

// failedHandshakeEvent occurs when a pending connection fails to handshake.
type failedHandshakeEvent struct {
	peerID   torlib.PeerID
	infoHash torlib.InfoHash
}

// Apply ejects the peer/hash of the failed handshake from the Scheduler's
// pending connections.
func (e failedHandshakeEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"peer": e.peerID, "hash": e.infoHash}).Debug("Applying failed handshake event")

	s.connState.DeletePending(e.peerID, e.infoHash)
	if err := s.connState.Blacklist(e.peerID, e.infoHash); err != nil {
		s.logf(log.Fields{
			"peer": e.peerID, "hash": e.infoHash,
		}).Infof("Error blacklisting pending conn: %s", err)
	}
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

	if err := s.connState.AddPending(e.handshake.PeerID, e.handshake.InfoHash); err != nil {
		s.logf(log.Fields{"handshake": e.handshake}).Errorf("Rejecting incoming handshake: %s", err)
		e.nc.Close()
		return
	}
	go s.initIncomingConn(e.nc, e.handshake)
}

// incomingConnEvent occurs when a pending incoming connection finishes handshaking.
type incomingConnEvent struct {
	conn    *conn
	torrent storage.Torrent
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
	s.networkEventProducer.Produce(
		networkevent.AddConnEvent(e.torrent.InfoHash(), s.pctx.PeerID, e.conn.PeerID))
}

// outgoingConnEvent occurs when a pending outgoing connection finishes handshaking.
type outgoingConnEvent struct {
	conn    *conn
	torrent storage.Torrent
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
	s.networkEventProducer.Produce(
		networkevent.AddConnEvent(e.torrent.InfoHash(), s.pctx.PeerID, e.conn.PeerID))
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
	infoHash torlib.InfoHash
	peers    []torlib.PeerInfo
}

// Apply selects new peers returned via an announce response to open connections to
// if there is capacity. These connections are added to the Scheduler's pending
// connections and handshaked asynchronously.
//
// Also marks the dispatcher as ready to announce again.
func (e announceResponseEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"hash": e.infoHash}).Debug("Applying announce response event")

	ctrl, ok := s.torrentControls[e.infoHash]
	if !ok {
		s.logf(log.Fields{
			"hash": e.infoHash,
		}).Info("Dispatcher closed after announce response received")
		return
	}
	s.announceQueue.Ready(ctrl.Dispatcher)
	for i := 0; i < len(e.peers); i++ {
		p := e.peers[i]
		pid, err := torlib.NewPeerID(p.PeerID)
		if err != nil {
			s.logf(log.Fields{
				"peer": p.PeerID, "hash": e.infoHash,
			}).Errorf("Error creating PeerID from announce response: %s", err)
			continue
		}
		if pid == s.pctx.PeerID {
			// Tracker may return our own peer.
			continue
		}
		s.logf(log.Fields{"peer": pid}).Debug("Received peer from tracker")
		if err := s.connState.AddPending(pid, e.infoHash); err != nil {
			if err == errTorrentAtCapacity {
				s.logf(log.Fields{
					"peer": pid, "hash": e.infoHash,
				}).Info("Cannot open any more connections, torrent is at capacity")
				break
			}
			s.logf(log.Fields{
				"peer": pid, "hash": e.infoHash,
			}).Infof("Cannot add pending conn: %s, skipping", err)
			continue
		}
		go s.initOutgoingConn(pid, p.IP, int(p.Port), ctrl.Dispatcher.Torrent)
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
	torrent storage.Torrent
	errc    chan error
}

// Apply begins seeding / leeching a new torrent.
func (e newTorrentEvent) Apply(s *Scheduler) {
	s.logf(log.Fields{"torrent": e.torrent}).Debug("Applying new torrent event")

	// If the torrent already exists, we must be careful not to use e.torrent
	// because using multiple Torrent instances for the same torrent file will
	// result in undefined behavior.

	infoHash := e.torrent.InfoHash()
	ctrl, ok := s.torrentControls[infoHash]
	if !ok {
		ctrl = newTorrentControl(s.dispatcherFactory.New(e.torrent))
		s.torrentControls[infoHash] = ctrl
		s.announceQueue.Add(ctrl.Dispatcher)
		s.connState.InitCapacity(infoHash)
		s.networkEventProducer.Produce(
			networkevent.AddTorrentEvent(infoHash, s.pctx.PeerID, e.torrent.Bitfield()))
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
	s.logf(log.Fields{"dispatcher": e.dispatcher}).Debug("Applying completed dispatcher event")

	infoHash := e.dispatcher.Torrent.InfoHash()

	s.announceQueue.Done(e.dispatcher)
	ctrl, ok := s.torrentControls[infoHash]
	if !ok {
		s.logf(log.Fields{"dispatcher": e.dispatcher}).Error("Completed dispatcher not found")
		return
	}
	for _, errc := range ctrl.Errors {
		errc <- nil
	}
	ctrl.Complete = true
	s.networkEventProducer.Produce(networkevent.TorrentCompleteEvent(infoHash, s.pctx.PeerID))
}

// preemptionTickEvent occurs periodically to preempt unneeded conns and remove
// idle torrentControls.
type preemptionTickEvent struct{}

func (e preemptionTickEvent) Apply(s *Scheduler) {
	s.log().Debug("Applying preemption tick event")

	for _, c := range s.connState.ActiveConns() {
		lastProgress := timeutil.MostRecent(
			c.CreatedAt, c.LastGoodPieceReceived(), c.LastPieceSent())
		if s.clock.Now().Sub(lastProgress) > s.config.IdleConnTTL {
			s.logf(log.Fields{"conn": c}).Info("Closing idle conn")
			c.Close()
			continue
		}
		if s.clock.Now().Sub(c.CreatedAt) > s.config.ConnTTL {
			s.logf(log.Fields{"conn": c}).Info("Closing expired conn")
			c.Close()
			continue
		}
	}

	for infoHash, ctrl := range s.torrentControls {
		if ctrl.Complete && ctrl.Dispatcher.Empty() {
			becameIdle := timeutil.MostRecent(
				ctrl.Dispatcher.CreatedAt, ctrl.Dispatcher.LastConnRemoved())
			if s.clock.Now().Sub(becameIdle) >= s.config.IdleSeederTTL {
				s.logf(log.Fields{"hash": infoHash}).Info("Removing idle torrent")
				delete(s.torrentControls, infoHash)
			}
		}
	}
}

// cleanupBlacklistEvent occurs periodically to allow the Scheduler to cleanup
// stale blacklist entries.
type cleanupBlacklistEvent struct{}

func (e cleanupBlacklistEvent) Apply(s *Scheduler) {
	s.log().Debug("Applying cleanup blacklist event")

	s.connState.DeleteStaleBlacklistEntries()
}

// emitStatsEvent occurs periodically to emit Scheduler stats.
type emitStatsEvent struct{}

func (e emitStatsEvent) Apply(s *Scheduler) {
	s.stats.Gauge("torrents").Update(float64(len(s.torrentControls)))
	s.stats.Gauge("conns").Update(float64(s.connState.NumActiveConns()))
}
