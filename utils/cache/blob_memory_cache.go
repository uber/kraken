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

	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/log"
)

// MemoryEntry represents a blob stored in memory cache.
type MemoryEntry struct {
	Name      string
	Data      []byte
	MetaInfo  *core.MetaInfo
	CreatedAt time.Time
}

// Size returns the size of the data in MemoryEntry
func (m *MemoryEntry) Size() uint64 {
	return uint64(len(m.Data))
}

// BlobMemoryCacheConfig defines configuration for BlobMemoryCache.
type BlobMemoryCacheConfig struct {
	MaxSize uint64 // Maximum memory in bytes
}

// BlobMemoryCache provides a simple in-memory cache for blob data with capacity management.
// It uses RWMutex for optimal concurrent access patterns.
type BlobMemoryCache struct {
	config BlobMemoryCacheConfig
	stats  tally.Scope

	// Storage
	mu        sync.RWMutex
	entries   map[string]*MemoryEntry
	totalSize uint64 // Includes both actual entries and reserved space
}

// NewBlobMemoryCache creates a new BlobMemoryCache with the specified configuration.
func NewBlobMemoryCache(
	config BlobMemoryCacheConfig,
	stats tally.Scope,
) *BlobMemoryCache {
	return &BlobMemoryCache{
		config:  config,
		stats:   stats.SubScope("blob_memory_cache"),
		entries: make(map[string]*MemoryEntry),
	}
}

// Add attempts to add an entry to the memory cache.
// NOTE: TryReserve must have succeeded, else unexpected behavior will happen
// This operation uses a write lock for exclusive access.
func (c *BlobMemoryCache) Add(entry *MemoryEntry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[entry.Name]; exists {
		// already exists, return
		// the caller should call release reservation if this happens else
		// there can be cases of over reservation
		return false
	}

	c.entries[entry.Name] = entry
	c.stats.Counter("entries_added").Inc(1)
	c.stats.Gauge("total_size_bytes").Update(float64(c.totalSize))
	return true
}

// Get retrieves an entry from the memory cache.
// Returns nil if not present.
// This operation uses a read lock, allowing concurrent access.
func (c *BlobMemoryCache) Get(name string) *MemoryEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[name]
	if !exists {
		c.stats.Counter("get_miss").Inc(1)
		return nil
	}

	c.stats.Counter("get_hit").Inc(1)
	return entry
}

// Remove removes an entry from the memory cache.
// No-op if entry is not present.
// This operation uses a write lock for exclusive access.
func (c *BlobMemoryCache) Remove(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.entries[name]
	if !exists {
		return // No-op
	}

	delete(c.entries, name)
	c.totalSize -= entry.Size()

	c.stats.Gauge("total_size_bytes").Update(float64(c.totalSize))
}

// NumEntries returns the current number of entries in the cache.
func (c *BlobMemoryCache) NumEntries() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// TotalBytes returns the current total size in bytes.
func (c *BlobMemoryCache) TotalBytes() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.totalSize
}

// TryReserve attempts to reserve space for buffering before adding to cache.
// Returns true if space was reserved, false if insufficient space.
// Reservation is tracked in totalSize to prevent OOM conditions.
func (c *BlobMemoryCache) TryReserve(size uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.totalSize+size > c.config.MaxSize {
		c.stats.Counter("reserve_failure").Inc(1)
		return false
	}
	c.totalSize += size // Reservation counts as part of totalSize
	return true
}

// ReleaseReservation releases previously reserved space.
// This is called when a reservation won't be used (e.g., download failed, entry already exists).
func (c *BlobMemoryCache) ReleaseReservation(size uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if size > c.totalSize {
		// This shouldn't happen
		log.With("size", size,
			"totalSize", c.totalSize).Error("unexpected behavior while releasing reservation")
		return
	}
	c.totalSize -= size
}

// GetExpiredEntries returns names of entries older than TTL.
// Uses RLock for minimal contention (allows concurrent reads).
func (c *BlobMemoryCache) GetExpiredEntries(now time.Time, ttl time.Duration) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var expiredNames []string

	for name, entry := range c.entries {
		if now.Sub(entry.CreatedAt) > ttl {
			expiredNames = append(expiredNames, name)
		}
	}

	return expiredNames
}

// RemoveBatch removes multiple entries atomically.
// Uses Lock for batch deletion (single lock acquisition for entire batch).
func (c *BlobMemoryCache) RemoveBatch(names []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, name := range names {
		entry, exists := c.entries[name]
		if !exists {
			continue
		}
		delete(c.entries, name)
		c.totalSize -= entry.Size()
	}
	c.stats.Gauge("total_size_bytes").Update(float64(c.totalSize))
}

// ListNames returns all entry names in the cache.
func (c *BlobMemoryCache) ListNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.entries))
	for name := range c.entries {
		names = append(names, name)
	}
	return names
}
