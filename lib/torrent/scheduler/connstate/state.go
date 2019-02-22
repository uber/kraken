// Copyright (c) 2019 Uber Technologies, Inc.
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
// limitations under the License.package connstate

import (
	"errors"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler/conn"
	"github.com/andres-erbsen/clock"
	"go.uber.org/zap"
)

// State errors.
var (
	ErrTorrentAtCapacity       = errors.New("torrent is at capacity")
	ErrConnAlreadyPending      = errors.New("conn is already pending")
	ErrConnAlreadyActive       = errors.New("conn is already active")
	ErrConnClosed              = errors.New("conn is closed")
	ErrInvalidActiveTransition = errors.New("conn must be pending to transition to active")
	ErrTooManyMutualConns      = errors.New("conn has too many mutual connections")
)

type connKey struct {
	peerID   core.PeerID
	infoHash core.InfoHash
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

// State provides connection lifecycle management and enforces connection
// limits. A connection to a peer is identified by peer id and torrent info hash.
// Each connection may exist in the following states: pending, active, or
// blacklisted. Pending connections are unestablished connections which "reserve"
// connection capacity until they are done handshaking. Active connections are
// established connections. Blacklisted connections are failed connections which
// should be skipped in each peer handout.
//
// Note, State is NOT thread-safe. Synchronization must be provided by the client.
type State struct {
	config      Config
	clk         clock.Clock
	netevents   networkevent.Producer
	localPeerID core.PeerID
	capacity    map[core.InfoHash]int
	active      map[connKey]*conn.Conn
	pending     map[connKey]bool
	blacklist   map[connKey]*blacklistEntry
	logger      *zap.SugaredLogger
}

// New creates a new State.
func New(
	config Config,
	clk clock.Clock,
	localPeerID core.PeerID,
	netevents networkevent.Producer,
	logger *zap.SugaredLogger) *State {

	config = config.applyDefaults()

	return &State{
		config:      config,
		clk:         clk,
		netevents:   netevents,
		localPeerID: localPeerID,
		capacity:    make(map[core.InfoHash]int),
		active:      make(map[connKey]*conn.Conn),
		pending:     make(map[connKey]bool),
		blacklist:   make(map[connKey]*blacklistEntry),
		logger:      logger,
	}
}

// MaxConnsPerTorrent returns the max number of connections a torrent is
// permitted to have.
func (s *State) MaxConnsPerTorrent() int {
	return s.config.MaxOpenConnectionsPerTorrent
}

// ActiveConns returns a list of all active connections.
func (s *State) ActiveConns() []*conn.Conn {
	conns := make([]*conn.Conn, len(s.active))
	var i int
	for _, c := range s.active {
		conns[i] = c
		i++
	}
	return conns
}

// NumActiveConns returns the total number of active connections.
func (s *State) NumActiveConns() int {
	return len(s.active)
}

// Blacklist blacklists peerID/h for the configured BlacklistDuration.
// Returns error if the connection is already blacklisted.
func (s *State) Blacklist(peerID core.PeerID, h core.InfoHash) error {
	if s.config.DisableBlacklist {
		return nil
	}

	k := connKey{peerID, h}
	if e, ok := s.blacklist[k]; ok && e.Blacklisted(s.clk.Now()) {
		return errors.New("conn is already blacklisted")
	}
	s.blacklist[k] = &blacklistEntry{s.clk.Now().Add(s.config.BlacklistDuration)}

	s.log("peer", peerID, "hash", h).Infof(
		"Connection blacklisted for %s", s.config.BlacklistDuration)
	s.netevents.Produce(
		networkevent.BlacklistConnEvent(h, s.localPeerID, peerID, s.config.BlacklistDuration))

	return nil
}

// Blacklisted returns true if peerID/h is blacklisted.
func (s *State) Blacklisted(peerID core.PeerID, h core.InfoHash) bool {
	e, ok := s.blacklist[connKey{peerID, h}]
	return ok && e.Blacklisted(s.clk.Now())
}

// ClearBlacklist un-blacklists all connections for h.
func (s *State) ClearBlacklist(h core.InfoHash) {
	for k := range s.blacklist {
		if k.infoHash == h {
			delete(s.blacklist, k)
		}
	}
}

// AddPending sets the connection for peerID/h as pending and reserves capacity
// for it.
func (s *State) AddPending(peerID core.PeerID, h core.InfoHash, neighbors []core.PeerID) error {
	k := connKey{peerID, h}
	cap, ok := s.capacity[h]
	if !ok {
		cap = s.config.MaxOpenConnectionsPerTorrent
		s.capacity[h] = cap
	}
	if cap == 0 {
		return ErrTorrentAtCapacity
	}
	if s.pending[k] {
		return ErrConnAlreadyPending
	}
	if _, ok := s.active[k]; ok {
		return ErrConnAlreadyActive
	}
	if s.numMutualConns(h, neighbors) > s.config.MaxMutualConnections {
		return ErrTooManyMutualConns
	}
	s.pending[k] = true
	s.capacity[k.infoHash]--

	s.log("peer", peerID, "hash", h).Infof(
		"Added pending conn, capacity now at %d", s.capacity[k.infoHash])

	return nil
}

// DeletePending deletes the pending connection for peerID/h and frees capacity.
func (s *State) DeletePending(peerID core.PeerID, h core.InfoHash) {
	k := connKey{peerID, h}
	if !s.pending[k] {
		return
	}
	delete(s.pending, k)
	s.capacity[k.infoHash]++

	s.log("peer", peerID, "hash", h).Infof(
		"Deleted pending conn, capacity now at %d", s.capacity[k.infoHash])
}

// MovePendingToActive sets a previously pending connection as active.
func (s *State) MovePendingToActive(c *conn.Conn) error {
	if c.IsClosed() {
		return ErrConnClosed
	}
	k := connKey{c.PeerID(), c.InfoHash()}
	if !s.pending[k] {
		return ErrInvalidActiveTransition
	}
	delete(s.pending, k)
	s.active[k] = c

	s.log("peer", k.peerID, "hash", k.infoHash).Info("Moved conn from pending to active")
	s.netevents.Produce(networkevent.AddActiveConnEvent(
		c.InfoHash(), s.localPeerID, c.PeerID()))

	return nil
}

// DeleteActive deletes c. No-ops if c is not an active conn.
func (s *State) DeleteActive(c *conn.Conn) {
	k := connKey{c.PeerID(), c.InfoHash()}
	cur, ok := s.active[k]
	if !ok || cur != c {
		// It is possible that some new conn shares the same connKey as the old conn,
		// so we need to make sure we're deleting the right one.
		return
	}
	delete(s.active, k)
	s.capacity[k.infoHash]++

	s.log("peer", k.peerID, "hash", k.infoHash).Infof(
		"Deleted active conn, capacity now at %d", s.capacity[k.infoHash])
	s.netevents.Produce(networkevent.DropActiveConnEvent(
		c.InfoHash(), s.localPeerID, c.PeerID()))

	return
}

func (s *State) numMutualConns(h core.InfoHash, neighbors []core.PeerID) int {
	var n int
	for _, id := range neighbors {
		if _, ok := s.active[connKey{id, h}]; ok {
			n++
		} else if _, ok := s.pending[connKey{id, h}]; ok {
			n++
		}
	}
	return n
}

// BlacklistedConn represents a connection which has been blacklisted.
type BlacklistedConn struct {
	PeerID    core.PeerID   `json:"peer_id"`
	InfoHash  core.InfoHash `json:"info_hash"`
	Remaining time.Duration `json:"remaining"`
}

// BlacklistSnapshot returns a snapshot of all valid blacklist entries.
func (s *State) BlacklistSnapshot() []BlacklistedConn {
	var conns []BlacklistedConn
	for k, e := range s.blacklist {
		c := BlacklistedConn{
			PeerID:    k.peerID,
			InfoHash:  k.infoHash,
			Remaining: e.Remaining(s.clk.Now()),
		}
		conns = append(conns, c)
	}
	return conns
}

func (s *State) log(args ...interface{}) *zap.SugaredLogger {
	return s.logger.With(args...)
}
