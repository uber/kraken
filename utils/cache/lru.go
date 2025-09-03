// Copyright (c) 2016-2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"sync"
	"time"
)

// LRUCache provides a simple LRU cache with both size and TTL limits.
// It uses RWMutex for optimal concurrent access patterns - multiple
// concurrent reads are allowed while writes get exclusive access.
type LRUCache struct {
	mu       sync.RWMutex
	entries  map[string]time.Time // key -> expiration time
	lruOrder []string             // keys in LRU order (oldest first)
	maxSize  int
	ttl      time.Duration
}

// NewLRUCache creates a new LRU cache with the specified maximum size and TTL.
func NewLRUCache(maxSize int, ttl time.Duration) *LRUCache {
	return &LRUCache{
		entries:  make(map[string]time.Time),
		lruOrder: make([]string, 0, maxSize),
		maxSize:  maxSize,
		ttl:      ttl,
	}
}

// Has checks if key exists and hasn't expired.
// This operation uses a read lock, allowing concurrent access.
func (c *LRUCache) Has(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	expireTime, exists := c.entries[key]
	if !exists || time.Now().After(expireTime) {
		return false
	}
	return true
}

// Add marks a key as cached. This operation uses a write lock for exclusive access.
// If the key already exists, its expiration time is updated and it's moved to the
// end of the LRU order. If the cache exceeds maxSize, oldest entries are evicted.
func (c *LRUCache) Add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	expireTime := now.Add(c.ttl)

	// If key already exists, update expiration and move to end
	if _, exists := c.entries[key]; exists {
		c.entries[key] = expireTime
		c.moveToEnd(key)
		return
	}

	// Add new entry
	c.entries[key] = expireTime
	c.lruOrder = append(c.lruOrder, key)

	// Evict expired and oldest entries if needed
	c.evict()
}

// Delete removes a key from the cache.
func (c *LRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[key]; !exists {
		return
	}

	delete(c.entries, key)
	for i, k := range c.lruOrder {
		if k == key {
			c.lruOrder = append(c.lruOrder[:i], c.lruOrder[i+1:]...)
			break
		}
	}
}

// Size returns the current number of entries in the cache.
func (c *LRUCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// Clear removes all entries from the cache.
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]time.Time)
	c.lruOrder = c.lruOrder[:0]
}

// evict removes expired entries and enforces size limit.
// This method assumes the caller already holds a write lock.
func (c *LRUCache) evict() {
	now := time.Now()

	// Remove expired entries
	i := 0
	for i < len(c.lruOrder) {
		key := c.lruOrder[i]
		if expireTime, exists := c.entries[key]; !exists || now.After(expireTime) {
			delete(c.entries, key)
			c.lruOrder = append(c.lruOrder[:i], c.lruOrder[i+1:]...)
		} else {
			i++
		}
	}

	// Enforce size limit by removing oldest entries
	for len(c.entries) > c.maxSize {
		oldest := c.lruOrder[0]
		delete(c.entries, oldest)
		c.lruOrder = c.lruOrder[1:]
	}
}

// moveToEnd moves key to end of LRU order.
// This method assumes the caller already holds a write lock.
func (c *LRUCache) moveToEnd(key string) {
	for i, k := range c.lruOrder {
		if k == key {
			c.lruOrder = append(append(c.lruOrder[:i], c.lruOrder[i+1:]...), key)
			break
		}
	}
}
