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
package connstate

import (
	"errors"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler/conn"
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

	// This should NEVER happen.
	errUnknownStatus = errors.New("invariant violation: unknown status")
)

type status int

const (
	// _uninit indicates the connection is uninitialized. This is the default
	// status for empty entries.
	_uninit status = iota
	_pending
	_active
)

type entry struct {
	status status
	conn   *conn.Conn
}

type connKey struct {
	hash   core.InfoHash
	peerID core.PeerID
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
// limits. A connection to a peer is identified by torrent info hash and peer id.
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
	logger      *zap.SugaredLogger

	// All pending or active conns. These count towards conn capacity.
	conns map[core.InfoHash]map[core.PeerID]entry

	// All blacklisted conns. These do not count towards conn capacity.
	blacklist map[connKey]*blacklistEntry
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
		logger:      logger,
		conns:       make(map[core.InfoHash]map[core.PeerID]entry),
		blacklist:   make(map[connKey]*blacklistEntry),
	}
}

// ActiveConns returns a list of all active connections.
func (s *State) ActiveConns() []*conn.Conn {
	var active []*conn.Conn
	for _, peers := range s.conns {
		for _, e := range peers {
			if e.status == _active {
				active = append(active, e.conn)
			}
		}
	}
	return active
}

// Saturated returns true if h is at capacity and all the conns are active.
func (s *State) Saturated(h core.InfoHash) bool {
	peers, ok := s.conns[h]
	if !ok {
		return false
	}
	var active int
	for _, e := range peers {
		if e.status == _active {
			active++
		}
	}
	return active == s.config.MaxOpenConnectionsPerTorrent
}

// Blacklist blacklists peerID/h for the configured BlacklistDuration.
// Returns error if the connection is already blacklisted.
func (s *State) Blacklist(peerID core.PeerID, h core.InfoHash) error {
	if s.config.DisableBlacklist {
		return nil
	}

	k := connKey{h, peerID}
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
	e, ok := s.blacklist[connKey{h, peerID}]
	return ok && e.Blacklisted(s.clk.Now())
}

// ClearBlacklist un-blacklists all connections for h.
func (s *State) ClearBlacklist(h core.InfoHash) {
	for k := range s.blacklist {
		if k.hash == h {
			delete(s.blacklist, k)
		}
	}
}

// AddPending sets the connection for peerID/h as pending and reserves capacity
// for it.
func (s *State) AddPending(peerID core.PeerID, h core.InfoHash, neighbors []core.PeerID) error {
	if len(s.conns[h]) == s.config.MaxOpenConnectionsPerTorrent {
		return ErrTorrentAtCapacity
	}
	switch s.get(h, peerID).status {
	case _uninit:
		if s.numMutualConns(h, neighbors) > s.config.MaxMutualConnections {
			return ErrTooManyMutualConns
		}
		s.put(h, peerID, entry{status: _pending})
		s.log("hash", h, "peer", peerID).Infof(
			"Added pending conn, capacity now at %d", s.capacity(h))
		return nil
	case _pending:
		return ErrConnAlreadyPending
	case _active:
		return ErrConnAlreadyActive
	default:
		return errUnknownStatus
	}
}

// DeletePending deletes the pending connection for peerID/h and frees capacity.
func (s *State) DeletePending(peerID core.PeerID, h core.InfoHash) {
	if s.get(h, peerID).status != _pending {
		return
	}
	s.delete(h, peerID)
	s.log("hash", h, "peer", peerID).Infof(
		"Deleted pending conn, capacity now at %d", s.capacity(h))
}

// MovePendingToActive sets a previously pending connection as active.
func (s *State) MovePendingToActive(c *conn.Conn) error {
	if c.IsClosed() {
		return ErrConnClosed
	}
	if s.get(c.InfoHash(), c.PeerID()).status != _pending {
		return ErrInvalidActiveTransition
	}
	s.put(c.InfoHash(), c.PeerID(), entry{status: _active, conn: c})

	s.log("hash", c.InfoHash(), "peer", c.PeerID()).Info("Moved conn from pending to active")
	s.netevents.Produce(networkevent.AddActiveConnEvent(c.InfoHash(), s.localPeerID, c.PeerID()))

	return nil
}

// DeleteActive deletes c. No-ops if c is not an active conn.
func (s *State) DeleteActive(c *conn.Conn) {
	e := s.get(c.InfoHash(), c.PeerID())
	if e.status != _active {
		return
	}
	if e.conn != c {
		// It is possible that some new conn shares the same hash/peer as the old conn,
		// so we need to make sure we're deleting the right one.
		return
	}
	s.delete(c.InfoHash(), c.PeerID())

	s.log("hash", c.InfoHash(), "peer", c.PeerID()).Infof(
		"Deleted active conn, capacity now at %d", s.capacity(c.InfoHash()))
	s.netevents.Produce(networkevent.DropActiveConnEvent(
		c.InfoHash(), s.localPeerID, c.PeerID()))
}

func (s *State) numMutualConns(h core.InfoHash, neighbors []core.PeerID) int {
	var n int
	for _, id := range neighbors {
		e := s.get(h, id)
		if e.status == _pending || e.status == _active {
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
			InfoHash:  k.hash,
			Remaining: e.Remaining(s.clk.Now()),
		}
		conns = append(conns, c)
	}
	return conns
}

func (s *State) get(h core.InfoHash, peerID core.PeerID) entry {
	peers, ok := s.conns[h]
	if !ok {
		return entry{}
	}
	return peers[peerID]
}

func (s *State) put(h core.InfoHash, peerID core.PeerID, e entry) {
	peers, ok := s.conns[h]
	if !ok {
		peers = make(map[core.PeerID]entry)
		s.conns[h] = peers
	}
	peers[peerID] = e
}

func (s *State) delete(h core.InfoHash, peerID core.PeerID) {
	peers, ok := s.conns[h]
	if !ok {
		return
	}
	delete(peers, peerID)
	if len(peers) == 0 {
		delete(s.conns, h)
	}
}

func (s *State) capacity(h core.InfoHash) int {
	return s.config.MaxOpenConnectionsPerTorrent - len(s.conns[h])
}

func (s *State) log(args ...interface{}) *zap.SugaredLogger {
	return s.logger.With(args...)
}
