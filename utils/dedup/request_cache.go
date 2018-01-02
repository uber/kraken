package dedup

import (
	"errors"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
)

// RequestCacheConfig defines RequestCache configuration.
type RequestCacheConfig struct {
	NotFoundTTL     time.Duration `yaml:"not_found_ttl"`
	ErrorTTL        time.Duration `yaml:"error_ttl"`
	CleanupInterval time.Duration `yaml:"cleanup_interval"`
	NumWorkers      int           `yaml:"num_workers"`
	BusyTimeout     time.Duration `yaml:"busy_timeout"`
}

func (c *RequestCacheConfig) applyDefaults() {
	if c.NotFoundTTL == 0 {
		c.NotFoundTTL = 30 * time.Second
	}
	if c.ErrorTTL == 0 {
		c.ErrorTTL = 30 * time.Second
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = 15 * time.Second
	}
	if c.NumWorkers == 0 {
		c.NumWorkers = 10000
	}
	if c.BusyTimeout == 0 {
		c.BusyTimeout = 5 * time.Second
	}
}

// RequestCache errors.
var (
	ErrRequestPending = errors.New("request pending")
	ErrNotFound       = errors.New("resource not found")
	ErrWorkersBusy    = errors.New("no workers available to handle request")
)

type cachedError struct {
	err       error
	expiresAt time.Time
}

func (e *cachedError) expired(now time.Time) bool {
	return now.After(e.expiresAt)
}

// RequestCache tracks pending requests and caches errors for configurable TTLs.
// It is used to prevent request duplication and DDOS-ing external components.
// Each request is represented by an arbitrary id string determined by the user.
type RequestCache struct {
	config RequestCacheConfig
	clk    clock.Clock

	mu        sync.Mutex // Protects access to the following fields:
	pending   map[string]bool
	errors    map[string]*cachedError
	lastClean time.Time

	numWorkers chan struct{}
}

// NewRequestCache creates a new RequestCache.
func NewRequestCache(config RequestCacheConfig, clk clock.Clock) *RequestCache {
	config.applyDefaults()
	return &RequestCache{
		config:     config,
		clk:        clk,
		pending:    make(map[string]bool),
		errors:     make(map[string]*cachedError),
		lastClean:  clk.Now(),
		numWorkers: make(chan struct{}, config.NumWorkers),
	}
}

// Start concurrently runs f under the given request id. Any error returned by
// f will be cached for the configured TTL. If f returns ErrNotFound, the configured
// NotFoundTTL is used instead of the ErrorTTL. If there is already a function
// executing under id, Start returns ErrRequestPending. If there are no available
// workers to run f, Start returns ErrWorkersBusy.
func (c *RequestCache) Start(id string, f func() error) error {
	if err := c.reserve(id); err != nil {
		return err
	}
	if err := c.reserveWorker(); err != nil {
		c.release(id)
		return err
	}
	go func() {
		defer c.releaseWorker()
		c.run(id, f)
	}()
	return nil
}

func (c *RequestCache) reserve(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Periodically remove expired errors.
	if c.clk.Now().Sub(c.lastClean) > c.config.CleanupInterval {
		for id, cerr := range c.errors {
			if cerr.expired(c.clk.Now()) {
				delete(c.errors, id)
			}
		}
		c.lastClean = c.clk.Now()
	}

	if c.pending[id] {
		return ErrRequestPending
	}
	if cerr, ok := c.errors[id]; ok && !cerr.expired(c.clk.Now()) {
		return cerr.err
	}

	c.pending[id] = true

	return nil
}

func (c *RequestCache) run(id string, f func() error) {
	if err := f(); err != nil {
		c.error(id, err)
		return
	}
	c.release(id)
}

func (c *RequestCache) release(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.pending, id)
}

func (c *RequestCache) error(id string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var ttl time.Duration
	if err == ErrNotFound {
		ttl = c.config.NotFoundTTL
	} else {
		ttl = c.config.ErrorTTL
	}
	delete(c.pending, id)
	c.errors[id] = &cachedError{err: err, expiresAt: c.clk.Now().Add(ttl)}
}

func (c *RequestCache) reserveWorker() error {
	select {
	case c.numWorkers <- struct{}{}:
		return nil
	case <-c.clk.After(c.config.BusyTimeout):
		return ErrWorkersBusy
	}
}

func (c *RequestCache) releaseWorker() {
	<-c.numWorkers
}
