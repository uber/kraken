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
)

// MemoryEntry represents a blob stored in memory cache.
type MemoryEntry struct {
	Name      string
	Data      []byte
	MetaInfo  *core.MetaInfo
	Size      int64
	CreatedAt time.Time
}

// BlobMemoryCacheConfig defines configuration for BlobMemoryCache.
type BlobMemoryCacheConfig struct {
	MaxSize int64 // Maximum memory in bytes
}

// BlobMemoryCache provides a simple in-memory cache for blob data with capacity management.
// It uses RWMutex for optimal concurrent access patterns.
type BlobMemoryCache struct {
	config BlobMemoryCacheConfig
	stats  tally.Scope

	// Storage
	mu        sync.RWMutex
	entries   map[string]*MemoryEntry
	totalSize int64
}

// NewBlobMemoryCache creates a new BlobMemoryCache with the specified configuration.
func NewBlobMemoryCache(
	config BlobMemoryCacheConfig,
	stats tally.Scope,
) *BlobMemoryCache {
	return &BlobMemoryCache{
		config:  config,
		stats:   stats.Tagged(map[string]string{"component": "blob_memory_cache"}),
		entries: make(map[string]*MemoryEntry),
	}
}

// Add attempts to add an entry to the memory cache.
// Returns true if successfully added, false if insufficient space.
// This operation uses a write lock for exclusive access.
func (c *BlobMemoryCache) Add(entry *MemoryEntry) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already exists
	if _, exists := c.entries[entry.Name]; exists {
		return true, nil // Already in cache, success
	}

	// Check capacity
	if c.totalSize+entry.Size > c.config.MaxSize {
		return false, nil // Not enough space, return
	}

	// Add to cache
	c.entries[entry.Name] = entry
	c.totalSize += entry.Size

	c.stats.Counter("entries_added").Inc(1)
	c.stats.Gauge("total_size_bytes").Update(float64(c.totalSize))

	return true, nil
}

// Get retrieves an entry from the memory cache.
// Returns nil if not present.
// This operation uses a read lock, allowing concurrent access.
func (c *BlobMemoryCache) Get(name string) *MemoryEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[name]
	if !exists {
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
	c.totalSize -= entry.Size

	c.stats.Gauge("total_size_bytes").Update(float64(c.totalSize))
}

// Size returns the current number of entries in the cache.
func (c *BlobMemoryCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// TotalBytes returns the current total size in bytes.
func (c *BlobMemoryCache) TotalBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.totalSize
}
