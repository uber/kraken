package dedup

import (
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
)

// CacheConfig defines configuration for Cache.
type CacheConfig struct {
	TTL             time.Duration `yaml:"ttl"`
	ErrorTTL        time.Duration `yaml:"error_ttl"`
	CleanupInterval time.Duration `yaml:"cleanup_interval"`
}

func (c CacheConfig) applyDefaults() CacheConfig {
	if c.TTL == 0 {
		c.TTL = 12 * time.Hour
	}
	if c.ErrorTTL == 0 {
		c.ErrorTTL = 30 * time.Second
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = 15 * time.Second
	}
	return c
}

// Resolver is used by Cache to resolve keys into values / errors.
type Resolver interface {
	Resolve(key string) (val string, err error)
}

type result struct {
	sync.RWMutex
	val       string
	err       error
	expiresAt time.Time
}

func (r *result) expired(now time.Time) bool {
	r.RLock()
	defer r.RUnlock()
	return now.After(r.expiresAt)
}

type cleanupManager struct {
	sync.RWMutex
	clk      clock.Clock
	interval time.Duration
	prev     time.Time
}

func (c *cleanupManager) _ready() bool {
	return c.clk.Now().After(c.prev.Add(c.interval))
}

// ready quickly checks if c is ready for the next cleanup.
func (c *cleanupManager) ready() bool {
	c.RLock()
	defer c.RUnlock()
	return c._ready()
}

// do executes f if c is ready for the next cleanup.
func (c *cleanupManager) do(f func()) {
	c.Lock()
	defer c.Unlock()
	if !c._ready() {
		return
	}
	f()
	c.prev = c.clk.Now()
}

// Cache deduplicates and memoizes key to value lookups using a Resolver.
type Cache struct {
	sync.RWMutex
	config         CacheConfig
	clk            clock.Clock
	resolver       Resolver
	results        map[string]*result
	cleanupManager *cleanupManager
}

// NewCache creates a new Cache. The given resolver will be used to perform key
// to value lookups.
func NewCache(config CacheConfig, clk clock.Clock, resolver Resolver) *Cache {
	config = config.applyDefaults()
	return &Cache{
		config:   config,
		clk:      clk,
		resolver: resolver,
		results:  make(map[string]*result),
		cleanupManager: &cleanupManager{
			clk:      clk,
			interval: config.CleanupInterval,
			prev:     clk.Now(),
		},
	}
}

// Get performs a key lookup using c's Resolver. Guarantees that no matter how
// many concurrent calls to Get are made, there will be exactly one Resolve(key)
// call within the configured TTL if Resolve(key) returns nil error, else within
// the configured ErrorTTL if Resolve(key) returns non-nil error.
func (c *Cache) Get(key string) (val string, err error) {

	// Very fast call which periodically deletes expired cache items.
	c.maybeClean()

	// Quickly check for a cached result under global read lock.
	c.RLock()
	r, ok := c.results[key]
	c.RUnlock()
	if ok {
		r.RLock()
		defer r.RUnlock()
		return r.val, r.err
	}

	// No cached result -- acquire the global write lock to initialize a result
	// struct and add it to cache.
	c.Lock()

	if r, ok := c.results[key]; ok {
		// Result struct was added before we could acquire the global write lock.
		c.Unlock()
		r.RLock()
		defer r.RUnlock()
		return r.val, r.err
	}

	r = &result{}
	r.Lock()
	defer r.Unlock()
	c.results[key] = r

	// Release the global lock while we still hold a write lock on r. Other
	// threads getting this key will be able to lookup r but will block on its
	// read lock until we are finished resolving the key.
	c.Unlock()

	r.val, r.err = c.resolver.Resolve(key)
	if r.err != nil {
		r.expiresAt = c.clk.Now().Add(c.config.ErrorTTL)
	} else {
		r.expiresAt = c.clk.Now().Add(c.config.TTL)
	}
	return r.val, r.err
}

func (c *Cache) maybeClean() {
	if !c.cleanupManager.ready() {
		return
	}
	c.cleanupManager.do(func() {
		c.Lock()
		defer c.Unlock()

		for k, r := range c.results {
			if r.expired(c.clk.Now()) {
				delete(c.results, k)
			}
		}
	})
}
