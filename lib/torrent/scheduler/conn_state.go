package scheduler

import (
	"errors"
	"fmt"
	"time"

	"github.com/andres-erbsen/clock"
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"
)

var errTorrentAtCapacity = errors.New("torrent is at capacity")

type blacklistError struct {
	remaining time.Duration
}

func (e blacklistError) Error() string {
	return fmt.Sprintf("conn is blacklisted for another %.1f seconds", e.remaining.Seconds())
}

type connKey struct {
	peerID   torlib.PeerID
	infoHash torlib.InfoHash
}

type blacklistEntry struct {
	expiration time.Time
}

func (e *blacklistEntry) Blacklisted(now time.Time) bool {
	return e.Remaining(now) > 0
}

func (e *blacklistEntry) Remaining(now time.Time) time.Duration {
	return e.expiration.Sub(now)
}

type connState struct {
	localPeerID   torlib.PeerID
	config        ConnStateConfig
	capacity      map[torlib.InfoHash]int
	active        map[connKey]*conn.Conn
	pending       map[connKey]bool
	blacklist     map[connKey]*blacklistEntry
	clock         clock.Clock
	networkEvents networkevent.Producer
}

func newConnState(
	localPeerID torlib.PeerID,
	config ConnStateConfig,
	clk clock.Clock,
	networkEvents networkevent.Producer) *connState {

	return &connState{
		localPeerID:   localPeerID,
		config:        config,
		capacity:      make(map[torlib.InfoHash]int),
		active:        make(map[connKey]*conn.Conn),
		pending:       make(map[connKey]bool),
		blacklist:     make(map[connKey]*blacklistEntry),
		clock:         clk,
		networkEvents: networkEvents,
	}
}

func (s *connState) ActiveConns() []*conn.Conn {
	conns := make([]*conn.Conn, 0, len(s.active))
	for _, c := range s.active {
		conns = append(conns, c)
	}
	return conns
}

func (s *connState) NumActiveConns() int {
	return len(s.active)
}

func (s *connState) Blacklist(peerID torlib.PeerID, infoHash torlib.InfoHash) error {
	if s.config.DisableBlacklist {
		return nil
	}

	k := connKey{peerID, infoHash}
	if e, ok := s.blacklist[k]; ok && e.Blacklisted(s.clock.Now()) {
		return errors.New("conn is already blacklisted")
	}
	s.blacklist[k] = &blacklistEntry{s.clock.Now().Add(s.config.BlacklistDuration)}

	s.log("peer", peerID, "hash", infoHash).Infof(
		"Connection blacklisted for %s", s.config.BlacklistDuration)
	s.networkEvents.Produce(
		networkevent.BlacklistConnEvent(infoHash, s.localPeerID, peerID, s.config.BlacklistDuration))

	return nil
}

func (s *connState) Blacklisted(peerID torlib.PeerID, infoHash torlib.InfoHash) bool {
	e, ok := s.blacklist[connKey{peerID, infoHash}]
	return ok && e.Blacklisted(s.clock.Now())
}

func (s *connState) ClearBlacklist(h torlib.InfoHash) {
	for k := range s.blacklist {
		if k.infoHash == h {
			delete(s.blacklist, k)
		}
	}
}

func (s *connState) AddPending(peerID torlib.PeerID, infoHash torlib.InfoHash) error {
	k := connKey{peerID, infoHash}
	cap, ok := s.capacity[infoHash]
	if !ok {
		cap = s.config.MaxOpenConnectionsPerTorrent
		s.capacity[infoHash] = cap
	}
	if cap == 0 {
		return errTorrentAtCapacity
	}
	if s.pending[k] {
		return errors.New("conn is already pending")
	}
	if _, ok := s.active[k]; ok {
		return errors.New("conn is already active")
	}
	s.pending[k] = true
	s.capacity[k.infoHash]--

	s.log("peer", peerID, "hash", infoHash).Infof(
		"Added pending conn, capacity now at %d", s.capacity[k.infoHash])
	s.networkEvents.Produce(networkevent.AddPendingConnEvent(infoHash, s.localPeerID, peerID))

	return nil
}

func (s *connState) DeletePending(peerID torlib.PeerID, infoHash torlib.InfoHash) {
	k := connKey{peerID, infoHash}
	if !s.pending[k] {
		return
	}
	delete(s.pending, k)
	s.capacity[k.infoHash]++

	s.log("peer", peerID, "hash", infoHash).Infof(
		"Deleted pending conn, capacity now at %d", s.capacity[k.infoHash])
	s.networkEvents.Produce(networkevent.DropPendingConnEvent(infoHash, s.localPeerID, peerID))
}

func (s *connState) MovePendingToActive(c *conn.Conn) error {
	k := connKey{c.PeerID(), c.InfoHash()}
	if !s.pending[k] {
		return errors.New("conn must be pending to transition to active")
	}
	delete(s.pending, k)
	if existingConn, ok := s.active[k]; ok {
		// If a connection already exists for this peer, we may preempt the
		// existing connection. This is to prevent the case where two peers,
		// A and B, both initialize connections to each other at the exact
		// same time. If neither connection is tramsitting data yet, the peers
		// independently agree on which connection should be kept by selecting
		// the connection opened by the peer with the larger peer id.
		if !s.newConnPreferred(existingConn, c) {
			s.capacity[k.infoHash]--
			return errors.New("conn already exists")
		}
		s.log("conn", existingConn).Info("Closing conflicting connection")
		existingConn.Close()
	}
	s.active[k] = c
	s.adjustConnBandwidthLimits()

	s.log("peer", k.peerID, "hash", k.infoHash).Info("Moved conn from pending to active")
	s.networkEvents.Produce(networkevent.AddActiveConnEvent(
		c.InfoHash(), s.localPeerID, c.PeerID()))

	return nil
}

// DeleteActive deletes c. No-ops if c is not an active conn.
func (s *connState) DeleteActive(c *conn.Conn) {
	k := connKey{c.PeerID(), c.InfoHash()}
	cur, ok := s.active[k]
	if !ok || cur != c {
		// It is possible that some new conn shares the same key as the old conn,
		// so we need to make sure we're deleting the right one.
		return
	}
	delete(s.active, k)
	s.capacity[k.infoHash]++
	s.adjustConnBandwidthLimits()

	s.log("peer", k.peerID, "hash", k.infoHash).Infof(
		"Deleted active conn, capacity now at %d", s.capacity[k.infoHash])
	s.networkEvents.Produce(networkevent.DropActiveConnEvent(
		c.InfoHash(), s.localPeerID, c.PeerID()))

	return
}

func (s *connState) BlacklistSnapshot() []BlacklistedConn {
	var conns []BlacklistedConn
	for k, e := range s.blacklist {
		c := BlacklistedConn{
			PeerID:    k.peerID,
			InfoHash:  k.infoHash,
			Remaining: e.Remaining(s.clock.Now()),
		}
		conns = append(conns, c)
	}
	return conns
}

// getConnOpener returns the PeerID of the peer who opened the conn, i.e. sent the first handshake.
func (s *connState) getConnOpener(c *conn.Conn) torlib.PeerID {
	if c.OpenedByRemote() {
		return c.PeerID()
	}
	return s.localPeerID
}

// If a connection already exists for this peer, we may preempt the existing connection. This
// is to prevent the case where two peers, A and B, both initialize connections to each other
// at the exact same time. If neither connection is tramsitting data yet, the peers independently
// agree on which connection should be kept by selecting the connection opened by the peer
// with the larger peer id.
func (s *connState) newConnPreferred(existingConn *conn.Conn, newConn *conn.Conn) bool {
	existingOpener := s.getConnOpener(existingConn)
	newOpener := s.getConnOpener(newConn)

	return existingOpener != newOpener &&
		!existingConn.Active() &&
		existingOpener.LessThan(newOpener)
}

// adjustConnBandwidthLimits balances the amount of egress bandwidth allocated to
// each active conn.
func (s *connState) adjustConnBandwidthLimits() {
	max := s.config.MaxGlobalEgressBytesPerSec
	min := s.config.MinConnEgressBytesPerSec
	n := uint64(len(s.active))
	if n == 0 {
		// No-op.
		return
	}
	limit := max / n
	if limit < min {
		// TODO(codyg): This is really bad. We need to either alert when this happens,
		// or throttle the number of torrents being added to the Scheduler.
		s.log().Errorf("Violating max global egress bandwidth by %d b/sec", min*n-max)
		limit = min
	}
	for _, c := range s.active {
		c.SetEgressBandwidthLimit(limit)
	}
	s.log().Infof("Balanced egress bandwidth to %s/sec across %d conns", memsize.Format(limit), n)
}

func (s *connState) log(args ...interface{}) *zap.SugaredLogger {
	args = append(args, "scheduler", s.localPeerID)
	return log.With(args...)
}
