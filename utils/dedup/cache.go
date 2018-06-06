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
	Resolve(ctx interface{}, key interface{}) (val interface{}, err error)
}

type result struct {
	sync.RWMutex
	val       interface{}
	err       error
	expiresAt time.Time
}

func (r *result) expired(now time.Time) bool {
	return now.After(r.expiresAt)
}

// Cache deduplicates and memoizes key to value lookups using a Resolver.
type Cache struct {
	sync.RWMutex
	config   CacheConfig
	clk      clock.Clock
	resolver Resolver
	results  map[interface{}]*result
	cleanup  *IntervalTrap
}

// NewCache creates a new Cache. The given resolver will be used to perform key
// to value lookups.
func NewCache(config CacheConfig, clk clock.Clock, resolver Resolver) *Cache {
	config = config.applyDefaults()
	cache := &Cache{
		config:   config,
		clk:      clk,
		resolver: resolver,
		results:  make(map[interface{}]*result),
	}
	cache.cleanup = NewIntervalTrap(config.CleanupInterval, clk, &cacheCleanup{cache})
	return cache
}

// Get performs a key lookup using c's Resolver. Guarantees that no matter how
// many concurrent calls to Get are made, there will be exactly one Resolve(ctx, key)
// call within the configured TTL if Resolve(ctx, key) returns nil error, else
// within the configured ErrorTTL if Resolve(ctx, key) returns non-nil error.
// ctx is the context for resolving a key in resolver.
func (c *Cache) Get(ctx interface{}, key interface{}) (val interface{}, err error) {
	c.cleanup.Trap()

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

	r.val, r.err = c.resolver.Resolve(ctx, key)
	if r.err != nil {
		r.expiresAt = c.clk.Now().Add(c.config.ErrorTTL)
	} else {
		r.expiresAt = c.clk.Now().Add(c.config.TTL)
	}
	return r.val, r.err
}

type cacheCleanup struct {
	cache *Cache
}

func (c *cacheCleanup) Run() {
	c.cache.Lock()
	defer c.cache.Unlock()

	for k, r := range c.cache.results {
		r.RLock()
		expired := r.expired(c.cache.clk.Now())
		r.RUnlock()
		if expired {
			delete(c.cache.results, k)
		}
	}
}
