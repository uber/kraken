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
	// TODO(codyg): If the cached error TTL is lower than the interval in which
	// clients are polling a 202 endpoint, then it is possible that the client
	// will never hit the actual error because it expires in between requests.
	if c.NotFoundTTL == 0 {
		c.NotFoundTTL = 15 * time.Second
	}
	if c.ErrorTTL == 0 {
		c.ErrorTTL = 15 * time.Second
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = 5 * time.Second
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
	ErrWorkersBusy    = errors.New("no workers available to handle request")
)

type cachedError struct {
	err       error
	expiresAt time.Time
}

func (e *cachedError) expired(now time.Time) bool {
	return now.After(e.expiresAt)
}

// Request defines functions which encapsulate a request.
type Request func() error

// ErrorMatcher defines functions which RequestCache uses to detect user defined
// errors.
type ErrorMatcher func(error) bool

// RequestCache tracks pending requests and caches errors for configurable TTLs.
// It is used to prevent request duplication and DDOS-ing external components.
// Each request is represented by an arbitrary id string determined by the user.
type RequestCache struct {
	config RequestCacheConfig
	clk    clock.Clock

	mu         sync.Mutex // Protects access to the following fields:
	pending    map[string]bool
	errors     map[string]*cachedError
	lastClean  time.Time
	isNotFound ErrorMatcher

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
		isNotFound: func(error) bool { return false },
		numWorkers: make(chan struct{}, config.NumWorkers),
	}
}

// SetNotFound sets the ErrorMatcher for activating the configured NotFoundTTL
// for errors returned by Request functions.
func (c *RequestCache) SetNotFound(m ErrorMatcher) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.isNotFound = m
}

// Start concurrently runs r under the given id. Any error returned by r will be
// cached for the configured TTL. If there is already a function executing under
// id, Start returns ErrRequestPending. If there are no available workers to run
// r, Start returns ErrWorkersBusy.
func (c *RequestCache) Start(id string, r Request) error {
	if err := c.reserve(id); err != nil {
		return err
	}
	if err := c.reserveWorker(); err != nil {
		c.release(id)
		return err
	}
	go func() {
		defer c.releaseWorker()
		c.run(id, r)
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

func (c *RequestCache) run(id string, r Request) {
	if err := r(); err != nil {
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
	if c.isNotFound(err) {
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
