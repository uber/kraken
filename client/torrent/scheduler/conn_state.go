package scheduler

import (
	"errors"
	"fmt"
	"math"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/client/torrent/meta"
	"github.com/uber-common/bark"
)

var errTorrentAtCapacity = errors.New("torrent is at capacity")

type blacklistError struct {
	remaining time.Duration
}

func (e blacklistError) Error() string {
	return fmt.Sprintf("conn is blacklisted for another %.1f seconds", e.remaining.Seconds())
}

type connKey struct {
	peerID   PeerID
	infoHash meta.Hash
}

type blacklistEntry struct {
	expiration time.Time
	failures   int
}

func (e *blacklistEntry) Blacklisted(now time.Time) bool {
	return e.Remaining(now) > 0
}

func (e *blacklistEntry) Remaining(now time.Time) time.Duration {
	return e.expiration.Sub(now)
}

type connState struct {
	localPeerID PeerID
	config      Config
	capacity    map[meta.Hash]int
	active      map[connKey]*conn
	pending     map[connKey]bool
	blacklist   map[connKey]*blacklistEntry

	// Allow overriding time.Now() for testing purposes.
	now func() time.Time
}

func newConnState(localPeerID PeerID, config Config) *connState {
	return &connState{
		localPeerID: localPeerID,
		config:      config,
		capacity:    make(map[meta.Hash]int),
		active:      make(map[connKey]*conn),
		pending:     make(map[connKey]bool),
		blacklist:   make(map[connKey]*blacklistEntry),
		now:         time.Now,
	}
}

func (s *connState) InitCapacity(infoHash meta.Hash) {
	s.capacity[infoHash] = s.config.MaxOpenConnectionsPerTorrent
}

func (s *connState) ActiveConns() []*conn {
	conns := make([]*conn, 0, len(s.active))
	for _, c := range s.active {
		conns = append(conns, c)
	}
	return conns
}

func (s *connState) Blacklist(peerID PeerID, infoHash meta.Hash) error {
	k := connKey{peerID, infoHash}
	e, ok := s.blacklist[k]
	if ok && e.Blacklisted(s.now()) {
		return errors.New("conn is already blacklisted")
	}
	if !ok {
		e = &blacklistEntry{}
		s.blacklist[k] = e
	}
	n := math.Ceil(math.Pow(s.config.BlacklistExpirationBackoff, float64(e.failures))) - 1
	d := s.config.InitialBlacklistExpiration + time.Duration(n)*time.Second
	if d > s.config.MaxBlacklistExpiration {
		d = s.config.MaxBlacklistExpiration
	} else if d < s.config.InitialBlacklistExpiration {
		s.log().Errorf("Invalid backoff calculation: got %.2f seconds, must be at least %.2f seconds",
			d.Seconds(), s.config.InitialBlacklistExpiration.Seconds())
		d = s.config.InitialBlacklistExpiration
	}
	e.expiration = s.now().Add(d)
	e.failures++
	s.logf(log.Fields{
		"peer": peerID, "hash": infoHash,
	}).Infof("Conn blacklisted for %.1f seconds after %d failures", d.Seconds(), e.failures)
	return nil
}

func (s *connState) AddPending(peerID PeerID, infoHash meta.Hash) error {
	k := connKey{peerID, infoHash}
	if e, ok := s.blacklist[k]; ok {
		now := s.now()
		if e.Blacklisted(now) {
			return blacklistError{remaining: e.Remaining(now)}
		}
	}
	if s.capacity[k.infoHash] == 0 {
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
	s.logf(log.Fields{
		"peer": peerID, "hash": infoHash,
	}).Infof("Adding pending conn, capacity now at %d", s.capacity[k.infoHash])
	return nil
}

func (s *connState) DeletePending(peerID PeerID, infoHash meta.Hash) {
	k := connKey{peerID, infoHash}
	if !s.pending[k] {
		return
	}
	delete(s.pending, k)
	s.capacity[k.infoHash]++
	s.logf(log.Fields{
		"peer": peerID, "hash": infoHash,
	}).Infof("Deleting pending conn, capacity now at %d", s.capacity[k.infoHash])
}

func (s *connState) MovePendingToActive(c *conn) error {
	k := connKey{c.PeerID, c.InfoHash}
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
		existingConn.Close()
	}
	s.active[k] = c
	s.logf(log.Fields{
		"peer": k.peerID, "hash": k.infoHash,
	}).Info("Moving conn from pending to active")
	return nil
}

func (s *connState) DeleteActive(c *conn) {
	k := connKey{c.PeerID, c.InfoHash}
	if cur, ok := s.active[k]; ok && cur == c {
		// It is possible that some new conn shares the same key as the old conn,
		// so we need to make sure we're deleting the right one.
		delete(s.active, k)
		s.capacity[k.infoHash]++
		s.logf(log.Fields{
			"peer": k.peerID, "hash": k.infoHash,
		}).Infof("Deleting active conn, capacity now at %d", s.capacity[k.infoHash])
	}
}

func (s *connState) DeleteStaleBlacklistEntries() {
	for k, e := range s.blacklist {
		if s.now().Sub(e.expiration) >= s.config.ExpiredBlacklistEntryTTL {
			delete(s.blacklist, k)
		}
	}
}

// getConnOpener returns the PeerID of the peer who opened the conn, i.e. sent the first handshake.
func (s *connState) getConnOpener(c *conn) PeerID {
	if c.OpenedByRemote() {
		return c.PeerID
	}
	return s.localPeerID
}

// If a connection already exists for this peer, we may preempt the existing connection. This
// is to prevent the case where two peers, A and B, both initialize connections to each other
// at the exact same time. If neither connection is tramsitting data yet, the peers independently
// agree on which connection should be kept by selecting the connection opened by the peer
// with the larger peer id.
func (s *connState) newConnPreferred(existingConn *conn, newConn *conn) bool {
	existingOpener := s.getConnOpener(existingConn)
	newOpener := s.getConnOpener(newConn)

	return existingOpener != newOpener &&
		!existingConn.Active() &&
		existingOpener.LessThan(newOpener)
}

func (s *connState) logf(f log.Fields) bark.Logger {
	f["scheduler"] = s.localPeerID
	return log.WithFields(f)
}

func (s *connState) log() bark.Logger {
	return s.logf(log.Fields{})
}
