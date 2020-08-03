// Copyright (c) 2016-2020 Uber Technologies, Inc.
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
package peerstore

import (
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/uber/kraken/core"
)

const (
	_cleanupExpiredPeerEntriesInterval = 5 * time.Minute
	_cleanupExpiredPeerGroupsInterval  = time.Hour
)

// LocalStore is an in-memory Store implementation.
type LocalStore struct {
	config                          LocalConfig
	clk                             clock.Clock
	cleanupExpiredPeerEntriesTicker *time.Ticker
	cleanupExpiredPeerGroupsTicker  *time.Ticker

	stopOnce sync.Once
	stop     chan struct{}

	mu         sync.RWMutex
	peerGroups map[core.InfoHash]*peerGroup
}

type peerGroup struct {
	mu            sync.RWMutex
	peers         map[core.PeerID]*peerEntry
	lastExpiresAt time.Time
	deleted       bool
}

type peerEntry struct {
	ip        string
	port      int
	complete  bool
	expiresAt time.Time
}

// NewLocalStore creates a new LocalStore.
func NewLocalStore(config LocalConfig, clk clock.Clock) *LocalStore {
	config.applyDefaults()
	s := &LocalStore{
		config:                          config,
		clk:                             clk,
		cleanupExpiredPeerEntriesTicker: time.NewTicker(_cleanupExpiredPeerEntriesInterval),
		cleanupExpiredPeerGroupsTicker:  time.NewTicker(_cleanupExpiredPeerGroupsInterval),
		stop:                            make(chan struct{}),
		peerGroups:                      make(map[core.InfoHash]*peerGroup),
	}
	go s.cleanupTask()
	return s
}

// Close implements Store.
func (s *LocalStore) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
}

// GetPeers implements Store.
func (s *LocalStore) GetPeers(h core.InfoHash, n int) ([]*core.PeerInfo, error) {
	if n <= 0 {
		// Simpler for below logic to assume positive n.
		return nil, nil
	}

	s.mu.RLock()
	g, ok := s.peerGroups[h]
	s.mu.RUnlock()
	if !ok {
		return nil, nil
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.peers) < n {
		n = len(g.peers)
	}
	result := make([]*core.PeerInfo, 0, n)

	// We rely on random map iteration to pick n random peers.
	for id, p := range g.peers {
		// Note, we elect to return slightly expired entries rather than iterate
		// until we find n valid entries.
		result = append(result, core.NewPeerInfo(id, p.ip, p.port, false /* origin */, p.complete))
		if len(result) == n {
			break
		}
	}
	return result, nil
}

// UpdatePeer implements Store.
func (s *LocalStore) UpdatePeer(h core.InfoHash, p *core.PeerInfo) error {
	g := s.getOrInitLockedPeerGroup(h)
	defer g.mu.Unlock()

	expiresAt := s.clk.Now().Add(s.config.TTL)

	g.peers[p.PeerID] = &peerEntry{
		ip:        p.IP,
		port:      p.Port,
		complete:  p.Complete,
		expiresAt: expiresAt,
	}

	// Allows cleanupExpiredPeerGroups to quickly determine when the last
	// peerEntry expires.
	g.lastExpiresAt = expiresAt

	return nil
}

func (s *LocalStore) getOrInitLockedPeerGroup(h core.InfoHash) *peerGroup {
	// We must take care to handle a race condition against
	// cleanupExpiredPeerGroups. Consider two goroutines, A and B, where A
	// executes getOrInitLockedPeerGroup and B executes
	// cleanupExpiredPeerGroups:
	//
	// A: locks s.mu, reads g from s.peerGroups, unlocks s.mu
	// B: locks s.mu, locks g.mu, deletes g from s.peerGroups, unlocks g.mu
	// A: locks g.mu
	//
	// At this point, A is holding onto a peerGroup reference which has been
	// deleted from the peerGroups map, and thus has no choice but to attempt to
	// reload a new peerGroup. Since the cleanup interval is quite large, it is
	// *extremely* unlikely this for-loop will execute more than twice.
	for {
		s.mu.Lock()
		g, ok := s.peerGroups[h]
		if !ok {
			g = &peerGroup{
				peers:         make(map[core.PeerID]*peerEntry),
				lastExpiresAt: s.clk.Now().Add(s.config.TTL),
			}
			s.peerGroups[h] = g
		}
		s.mu.Unlock()

		g.mu.Lock()
		if g.deleted {
			g.mu.Unlock()
			continue
		}
		return g
	}
}

func (s *LocalStore) cleanupTask() {
	for {
		select {
		case <-s.cleanupExpiredPeerEntriesTicker.C:
			s.cleanupExpiredPeerEntries()
		case <-s.cleanupExpiredPeerGroupsTicker.C:
			s.cleanupExpiredPeerGroups()
		case <-s.stop:
			return
		}
	}
}

func (s *LocalStore) cleanupExpiredPeerEntries() {
	s.mu.RLock()
	hashes := make([]core.InfoHash, 0, len(s.peerGroups))
	for h := range s.peerGroups {
		hashes = append(hashes, h)
	}
	s.mu.RUnlock()

	for _, h := range hashes {
		s.mu.RLock()
		g, ok := s.peerGroups[h]
		s.mu.RUnlock()
		if !ok {
			continue
		}

		var expired []core.PeerID
		g.mu.RLock()
		for id, p := range g.peers {
			if s.clk.Now().After(p.expiresAt) {
				expired = append(expired, id)
			}
		}
		g.mu.RUnlock()

		if len(expired) == 0 {
			// Fast path -- no need to acquire a write lock if there are no
			// expired entries.
			continue
		}

		g.mu.Lock()
		for _, id := range expired {
			// Must re-check the expiresAt timestamp in case an update occurred
			// before we could acquire the write lock.
			p, ok := g.peers[id]
			if ok && s.clk.Now().After(p.expiresAt) {
				delete(g.peers, id)
			}
		}
		g.mu.Unlock()
	}
}

func (s *LocalStore) cleanupExpiredPeerGroups() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for h, g := range s.peerGroups {
		g.mu.RLock()
		valid := s.clk.Now().Before(g.lastExpiresAt)
		g.mu.RUnlock()

		if valid {
			// Fast path -- no need to acquire a write lock if the group is
			// still valid.
			continue
		}

		g.mu.Lock()
		// Must re-check the lastExpiresAt timestamp in case an update
		// occurred before we could acquire the write lock.
		if s.clk.Now().After(g.lastExpiresAt) {
			delete(s.peerGroups, h)
			g.deleted = true
		}
		g.mu.Unlock()
	}
}
