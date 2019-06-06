// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scheduler

import (
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler/conn"
	"github.com/uber/kraken/lib/torrent/scheduler/connstate"
	"github.com/uber/kraken/lib/torrent/scheduler/dispatch"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/utils/memsize"
	"github.com/uber/kraken/utils/timeutil"

	"github.com/willf/bitset"
)

// event describes an external event which modifies state. While the event is
// applying, it is guaranteed to be the only accessor of state.
type event interface {
	apply(*state)
}

// eventLoop represents a serialized list of events to be applied to scheduler
// state.
type eventLoop interface {
	send(event) bool
	sendTimeout(e event, timeout time.Duration) error
	run(*state)
	stop()
}

type baseEventLoop struct {
	events chan event
	done   chan struct{}
}

func newEventLoop() *baseEventLoop {
	return &baseEventLoop{
		events: make(chan event),
		done:   make(chan struct{}),
	}
}

// send sends a new event into l. Should never be called by the same goroutine
// running l (i.e. within apply methods), else deadlock will occur. Returns false
// if the l is not running.
func (l *baseEventLoop) send(e event) bool {
	select {
	case l.events <- e:
		return true
	case <-l.done:
		return false
	}
}

func (l *baseEventLoop) sendTimeout(e event, timeout time.Duration) error {
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

func (l *baseEventLoop) run(s *state) {
	for {
		select {
		case e := <-l.events:
			e.apply(s)
		case <-l.done:
			return
		}
	}
}

func (l *baseEventLoop) stop() {
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
	l.send(connClosedEvent{c})
}

func (l *liftedEventLoop) DispatcherComplete(d *dispatch.Dispatcher) {
	l.send(dispatcherCompleteEvent{d})
}

func (l *liftedEventLoop) PeerRemoved(peerID core.PeerID, h core.InfoHash) {
	l.send(peerRemovedEvent{peerID, h})
}

func (l *liftedEventLoop) AnnounceTick() {
	l.send(announceTickEvent{})
}

// connClosedEvent occurs when a connection is closed.
type connClosedEvent struct {
	c *conn.Conn
}

// apply ejects the conn from the scheduler's active connections.
func (e connClosedEvent) apply(s *state) {
	s.conns.DeleteActive(e.c)
	if err := s.conns.Blacklist(e.c.PeerID(), e.c.InfoHash()); err != nil {
		s.log("conn", e.c).Infof("Cannot blacklist active conn: %s", err)
	}
}

// incomingHandshakeEvent when a handshake was received from a new connection.
type incomingHandshakeEvent struct {
	pc *conn.PendingConn
}

// apply rejects incoming handshakes when the scheduler is at capacity. If the
// scheduler has capacity for more connections, adds the peer/hash of the handshake
// to the scheduler's pending connections and asynchronously attempts to establish
// the connection.
func (e incomingHandshakeEvent) apply(s *state) {
	peerNeighbors := make([]core.PeerID, len(e.pc.RemoteBitfields()))
	var i int
	for peerID := range e.pc.RemoteBitfields() {
		peerNeighbors[i] = peerID
		i++
	}
	if err := s.conns.AddPending(e.pc.PeerID(), e.pc.InfoHash(), peerNeighbors); err != nil {
		s.log("peer", e.pc.PeerID(), "hash", e.pc.InfoHash()).Infof(
			"Rejecting incoming handshake: %s", err)
		s.sched.torrentlog.IncomingConnectionReject(e.pc.Digest(), e.pc.InfoHash(), e.pc.PeerID(), err)
		e.pc.Close()
		return
	}
	var rb conn.RemoteBitfields
	if ctrl, ok := s.torrentControls[e.pc.InfoHash()]; ok {
		rb = ctrl.dispatcher.RemoteBitfields()
	}
	go s.sched.establishIncomingHandshake(e.pc, rb)
}

// failedIncomingHandshakeEvent occurs when a pending incoming connection fails
// to handshake.
type failedIncomingHandshakeEvent struct {
	peerID   core.PeerID
	infoHash core.InfoHash
}

func (e failedIncomingHandshakeEvent) apply(s *state) {
	s.conns.DeletePending(e.peerID, e.infoHash)
}

// incomingConnEvent occurs when a pending incoming connection finishes handshaking.
type incomingConnEvent struct {
	namespace string
	c         *conn.Conn
	bitfield  *bitset.BitSet
	info      *storage.TorrentInfo
}

// apply transitions a fully-handshaked incoming conn from pending to active.
func (e incomingConnEvent) apply(s *state) {
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

func (e failedOutgoingHandshakeEvent) apply(s *state) {
	s.conns.DeletePending(e.peerID, e.infoHash)
	if err := s.conns.Blacklist(e.peerID, e.infoHash); err != nil {
		s.log("peer", e.peerID, "hash", e.infoHash).Infof("Cannot blacklist pending conn: %s", err)
	}
}

// outgoingConnEvent occurs when a pending outgoing connection finishes handshaking.
type outgoingConnEvent struct {
	c        *conn.Conn
	bitfield *bitset.BitSet
	info     *storage.TorrentInfo
}

// apply transitions a fully-handshaked outgoing conn from pending to active.
func (e outgoingConnEvent) apply(s *state) {
	if err := s.addOutgoingConn(e.c, e.bitfield, e.info); err != nil {
		s.log("conn", e.c).Errorf("Error adding outgoing conn: %s", err)
		e.c.Close()
		return
	}
	s.log("conn", e.c).Infof("Added outgoing conn with %d%% downloaded", e.info.PercentDownloaded())
}

// announceTickEvent occurs when it is time to announce to the tracker.
type announceTickEvent struct{}

// apply pulls the next dispatcher from the announce queue and asynchronously
// makes an announce request to the tracker.
func (e announceTickEvent) apply(s *state) {
	var skipped []core.InfoHash
	for {
		h, ok := s.announceQueue.Next()
		if !ok {
			s.log().Debug("No torrents in announce queue")
			break
		}
		if s.conns.Saturated(h) {
			s.log("hash", h).Debug("Skipping announce for fully saturated torrent")
			skipped = append(skipped, h)
			continue
		}
		ctrl, ok := s.torrentControls[h]
		if !ok {
			s.log("hash", h).Error("Pulled unknown torrent off announce queue")
			continue
		}
		go s.sched.announce(
			ctrl.dispatcher.Digest(), ctrl.dispatcher.InfoHash(), ctrl.dispatcher.Complete())
		break
	}
	// Re-enqueue any torrents we pulled off and ignored, else we would never
	// announce them again.
	for _, h := range skipped {
		s.announceQueue.Ready(h)
	}
}

// announceResultEvent occurs when a successfully announced response was received
// from the tracker.
type announceResultEvent struct {
	infoHash core.InfoHash
	peers    []*core.PeerInfo
}

// apply selects new peers returned via an announce response to open connections to
// if there is capacity. These connections are added to the scheduler's pending
// connections and handshaked asynchronously.
//
// Also marks the dispatcher as ready to announce again.
func (e announceResultEvent) apply(s *state) {
	ctrl, ok := s.torrentControls[e.infoHash]
	if !ok {
		s.log("hash", e.infoHash).Info("Dispatcher closed after announce response received")
		return
	}
	s.announceQueue.Ready(e.infoHash)
	if ctrl.dispatcher.Complete() {
		// Torrent is already complete, don't open any new connections.
		return
	}
	for _, p := range e.peers {
		if p.PeerID == s.sched.pctx.PeerID {
			// Tracker may return our own peer.
			continue
		}
		if s.conns.Blacklisted(p.PeerID, e.infoHash) {
			continue
		}
		if err := s.conns.AddPending(p.PeerID, e.infoHash, nil); err != nil {
			if err == connstate.ErrTorrentAtCapacity {
				break
			}
			continue
		}
		go s.sched.initializeOutgoingHandshake(
			p, ctrl.dispatcher.Stat(), ctrl.dispatcher.RemoteBitfields(), ctrl.namespace)
	}
}

// announceErrEvent occurs when an announce request fails.
type announceErrEvent struct {
	infoHash core.InfoHash
	err      error
}

// apply marks the dispatcher as ready to announce again.
func (e announceErrEvent) apply(s *state) {
	s.log("hash", e.infoHash).Errorf("Error announcing: %s", e.err)
	s.announceQueue.Ready(e.infoHash)
}

// newTorrentEvent occurs when a new torrent was requested for download.
type newTorrentEvent struct {
	namespace string
	torrent   storage.Torrent
	errc      chan error
}

// apply begins seeding / leeching a new torrent.
func (e newTorrentEvent) apply(s *state) {
	ctrl, ok := s.torrentControls[e.torrent.InfoHash()]
	if !ok {
		var err error
		ctrl, err = s.addTorrent(e.namespace, e.torrent, true)
		if err != nil {
			e.errc <- err
			return
		}
		s.log("torrent", e.torrent).Info("Added new torrent")
	}
	if ctrl.dispatcher.Complete() {
		e.errc <- nil
		return
	}
	ctrl.errors = append(ctrl.errors, e.errc)

	// Immediately announce new torrents.
	go s.sched.announce(ctrl.dispatcher.Digest(), ctrl.dispatcher.InfoHash(), ctrl.dispatcher.Complete())
}

// dispatcherCompleteEvent occurs when a dispatcher finishes downloading its torrent.
type dispatcherCompleteEvent struct {
	dispatcher *dispatch.Dispatcher
}

// apply marks the dispatcher for its final announce.
func (e dispatcherCompleteEvent) apply(s *state) {
	infoHash := e.dispatcher.InfoHash()

	s.conns.ClearBlacklist(infoHash)
	s.announceQueue.Eject(infoHash)
	ctrl, ok := s.torrentControls[infoHash]
	if !ok {
		s.log("dispatcher", e.dispatcher).Error("Completed dispatcher not found")
		return
	}
	for _, errc := range ctrl.errors {
		errc <- nil
	}
	if ctrl.localRequest {
		// Normalize the download time for all torrent sizes to a per MB value.
		// Skip torrents that are less than a MB in size because we can't measure
		// at that granularity.
		downloadTime := s.sched.clock.Now().Sub(ctrl.dispatcher.CreatedAt())
		lengthMB := ctrl.dispatcher.Length() / int64(memsize.MB)
		if lengthMB > 0 {
			s.sched.stats.Timer("download_time_per_mb").Record(downloadTime / time.Duration(lengthMB))
		}
	}

	s.log("hash", infoHash).Info("Torrent complete")
	s.sched.netevents.Produce(networkevent.TorrentCompleteEvent(infoHash, s.sched.pctx.PeerID))

	// Immediately announce completed torrents.
	go s.sched.announce(ctrl.dispatcher.Digest(), ctrl.dispatcher.InfoHash(), true)
}

// peerRemovedEvent occurs when a dispatcher removes a peer with a closed
// connection. Currently is a no-op.
type peerRemovedEvent struct {
	peerID   core.PeerID
	infoHash core.InfoHash
}

func (e peerRemovedEvent) apply(s *state) {}

// preemptionTickEvent occurs periodically to preempt unneeded conns and remove
// idle torrentControls.
type preemptionTickEvent struct{}

func (e preemptionTickEvent) apply(s *state) {
	for _, c := range s.conns.ActiveConns() {
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
		if s.sched.clock.Now().Sub(lastProgress) > s.sched.config.ConnTTI {
			s.log("conn", c).Info("Closing idle conn")
			c.Close()
			continue
		}
		if s.sched.clock.Now().Sub(c.CreatedAt()) > s.sched.config.ConnTTL {
			s.log("conn", c).Info("Closing expired conn")
			c.Close()
			continue
		}
	}

	for h, ctrl := range s.torrentControls {
		idleSeeder :=
			ctrl.dispatcher.Complete() &&
				s.sched.clock.Now().Sub(ctrl.dispatcher.LastReadTime()) >= s.sched.config.SeederTTI
		if idleSeeder {
			s.sched.torrentlog.SeedTimeout(ctrl.dispatcher.Digest(), h)
		}

		idleLeecher :=
			!ctrl.dispatcher.Complete() &&
				s.sched.clock.Now().Sub(ctrl.dispatcher.LastWriteTime()) >= s.sched.config.LeecherTTI
		if idleLeecher {
			s.sched.torrentlog.LeechTimeout(ctrl.dispatcher.Digest(), h)
		}

		if idleSeeder || idleLeecher {
			s.log("hash", h, "inprogress", !ctrl.dispatcher.Complete()).Info("Removing idle torrent")
			s.removeTorrent(h, ErrTorrentTimeout)
		}
	}
}

// emitStatsEvent occurs periodically to emit scheduler stats.
type emitStatsEvent struct{}

func (e emitStatsEvent) apply(s *state) {
	s.sched.stats.Gauge("torrents").Update(float64(len(s.torrentControls)))
}

type blacklistSnapshotEvent struct {
	result chan []connstate.BlacklistedConn
}

func (e blacklistSnapshotEvent) apply(s *state) {
	e.result <- s.conns.BlacklistSnapshot()
}

// removeTorrentEvent occurs when a torrent is manually removed via scheduler API.
type removeTorrentEvent struct {
	digest core.Digest
	errc   chan error
}

func (e removeTorrentEvent) apply(s *state) {
	for h, ctrl := range s.torrentControls {
		if ctrl.dispatcher.Digest() == e.digest {
			s.log(
				"hash", h,
				"inprogress", !ctrl.dispatcher.Complete()).Info("Removing torrent")
			s.removeTorrent(h, ErrTorrentRemoved)
		}
	}
	e.errc <- s.sched.torrentArchive.DeleteTorrent(e.digest)
}

// probeEvent occurs when a probe is manually requested via scheduler API.
// The event loop is unbuffered, so if a probe can be successfully sent, then
// the event loop is healthy.
type probeEvent struct{}

func (e probeEvent) apply(*state) {}

// shutdownEvent stops the event loop and tears down all active torrents and
// connections.
type shutdownEvent struct{}

func (e shutdownEvent) apply(s *state) {
	for _, c := range s.conns.ActiveConns() {
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
	s.sched.eventLoop.stop()
}
