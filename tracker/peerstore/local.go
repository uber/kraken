package peerstore

import (
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/dedup"
)

const _peerGroupCleanupInterval = time.Hour

// LocalStore is an in-memory Store implementation.
type LocalStore struct {
	config  LocalConfig
	clk     clock.Clock
	cleanup *dedup.IntervalTrap

	mu         sync.RWMutex
	peerGroups map[core.InfoHash]*peerGroup
}

type peerGroup struct {
	mu            sync.Mutex
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

type cleanupTask struct {
	store *LocalStore
}

func (t *cleanupTask) Run() {
	t.store.runCleanup()
}

// NewLocalStore creates a new LocalStore.
func NewLocalStore(config LocalConfig, clk clock.Clock) *LocalStore {
	config.applyDefaults()
	s := &LocalStore{
		config:     config,
		clk:        clk,
		peerGroups: make(map[core.InfoHash]*peerGroup),
	}
	s.cleanup = dedup.NewIntervalTrap(_peerGroupCleanupInterval, clk, &cleanupTask{s})
	return s
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

	g.mu.Lock()
	defer g.mu.Unlock()

	now := s.clk.Now()
	var result []*core.PeerInfo

	// We rely on random map iteration to pick n random peers.
	for id, p := range g.peers {
		if now.After(p.expiresAt) {
			// Clean up any expired peers we run into.
			delete(g.peers, id)
			continue
		}
		result = append(result, core.NewPeerInfo(id, p.ip, p.port, false /* origin */, p.complete))
		if len(result) == n {
			break
		}
	}
	return result, nil
}

// UpdatePeer implements Store.
func (s *LocalStore) UpdatePeer(h core.InfoHash, p *core.PeerInfo) error {
	s.cleanup.Trap()

	g := s.getOrInitLockedPeerGroup(h)
	defer g.mu.Unlock()

	expiresAt := s.clk.Now().Add(s.config.TTL)

	g.peers[p.PeerID] = &peerEntry{
		ip:        p.IP,
		port:      p.Port,
		complete:  p.Complete,
		expiresAt: expiresAt,
	}

	// Allows runCleanup to quickly determine when the last peerEntry expires.
	g.lastExpiresAt = expiresAt

	return nil
}

func (s *LocalStore) getOrInitLockedPeerGroup(h core.InfoHash) *peerGroup {
	// We must take care to handle a race condition against runCleanup. Consider
	// two goroutines, A and B, where A executes getOrInitLockedPeerGroup and B
	// executes runCleanup:
	//
	// A: locks s.mu, reads g from s.peerGroups, unlocks s.mu
	// B: locks s.mu, locks g.mu, deletes g from s.peerGroups, unlocks g.mu
	// A: locks g.mu
	//
	// At this point, A is holding onto a peerGroup reference which has been
	// deleted from the peerGroups map, and thus has no choice but to attempt to
	// reload a new peerGroup. Since the cleanup interval is quite large, it is
	// *extremeley* unlikely this for-loop will execute more than twice.
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

func (s *LocalStore) runCleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for h, g := range s.peerGroups {
		g.mu.Lock()
		if s.clk.Now().After(g.lastExpiresAt) {
			delete(s.peerGroups, h)
			g.deleted = true
		}
		g.mu.Unlock()
	}
}
