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
	"math/rand"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/uber/kraken/core"
	_ "github.com/uber/kraken/utils/randutil" // For seeded global rand.
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
	mu sync.RWMutex

	// Same peerEntry references in both, just indexed differently.
	peerList []*peerEntry
	peerMap  map[core.PeerID]*peerEntry

	lastExpiresAt time.Time
	deleted       bool
}

type peerEntry struct {
	id        core.PeerID
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
	s.mu.RLock()
	g, ok := s.peerGroups[h]
	s.mu.RUnlock()
	if !ok {
		return nil, nil
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.peerList) < n {
		n = len(g.peerList)
	}
	if n <= 0 {
		return nil, nil
	}

	result := make([]*core.PeerInfo, 0, n)

	// Select n random indexes.
	indexes := rand.Perm(len(g.peerList))
	indexes = indexes[:n]

	for _, i := range indexes {
		// Note, we elect to return slightly expired entries rather than iterate
		// until we find n valid entries.
		e := g.peerList[i]
		result = append(result, core.NewPeerInfo(e.id, e.ip, e.port, false /* origin */, e.complete))
	}
	return result, nil
}

// UpdatePeer implements Store.
func (s *LocalStore) UpdatePeer(h core.InfoHash, p *core.PeerInfo) error {
	g := s.getOrInitLockedPeerGroup(h)
	defer g.mu.Unlock()

	e, ok := g.peerMap[p.PeerID]
	if !ok {
		e = &peerEntry{}
		g.peerList = append(g.peerList, e)
		g.peerMap[p.PeerID] = e
	}
	e.id = p.PeerID
	e.ip = p.IP
	e.port = p.Port
	e.complete = p.Complete
	e.expiresAt = s.clk.Now().Add(s.config.TTL)

	// Allows cleanupExpiredPeerGroups to quickly determine when the last
	// peerEntry expires.
	g.lastExpiresAt = e.expiresAt

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
				peerMap:       make(map[core.PeerID]*peerEntry),
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
	groups := make([]*peerGroup, 0, len(s.peerGroups))
	for _, g := range s.peerGroups {
		groups = append(groups, g)
	}
	s.mu.RUnlock()

	for _, g := range groups {
		var expired []int

		g.mu.RLock()
		for i, e := range g.peerList {
			if s.clk.Now().After(e.expiresAt) {
				expired = append(expired, i)
			}
		}
		g.mu.RUnlock()

		if len(expired) == 0 {
			// Fast path -- no need to acquire a write lock if there are no
			// expired entries.
			continue
		}

		g.mu.Lock()
		for j := len(expired) - 1; j >= 0; j-- {
			// Loop over expired indexes in reverse orders to perform fast slice
			// element removal.
			i := expired[j]

			if i >= len(g.peerList) {
				// Technically we're the only goroutine deleting peer entries,
				// but let's play it safe.
				continue
			}
			e := g.peerList[i]

			// Must re-check the expiresAt timestamp in case an update occurred
			// before we could acquire the write lock.
			if s.clk.Now().Before(e.expiresAt) {
				continue
			}

			// Remove the expired index.
			g.peerList[i] = g.peerList[len(g.peerList)-1]
			g.peerList = g.peerList[:len(g.peerList)-1]

			delete(g.peerMap, e.id)
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
