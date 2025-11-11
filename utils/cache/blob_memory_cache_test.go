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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
)

func TestBlobMemoryCache_Add(t *testing.T) {
	tests := []struct {
		name            string
		maxSize         uint64
		entries         []*MemoryEntry
		expectedAdded   []bool
		expectedEntries int
		expectedBytes   uint64
	}{
		{
			name:    "success",
			maxSize: 1000,
			entries: []*MemoryEntry{
				{Name: "blob1", Data: make([]byte, 500), CreatedAt: time.Now()},
			},
			expectedAdded:   []bool{true},
			expectedEntries: 1,
			expectedBytes:   500,
		},
		{
			name:    "insufficient space",
			maxSize: 1000,
			entries: []*MemoryEntry{
				{Name: "blob1", Data: make([]byte, 1500), CreatedAt: time.Now()},
			},
			expectedAdded:   []bool{false},
			expectedEntries: 0,
			expectedBytes:   0,
		},
		{
			name:    "already exists",
			maxSize: 2000,
			entries: []*MemoryEntry{
				{Name: "blob1", Data: make([]byte, 500), CreatedAt: time.Now()},
				{Name: "blob1", Data: make([]byte, 500), CreatedAt: time.Now()},
			},
			expectedAdded:   []bool{true, false}, // Second returns false (already exists)
			expectedEntries: 1,
			expectedBytes:   500,
		},
		{
			name:    "zero max size",
			maxSize: 0,
			entries: []*MemoryEntry{
				{Name: "blob1", Data: make([]byte, 100), CreatedAt: time.Now()},
			},
			expectedAdded:   []bool{false},
			expectedEntries: 0,
			expectedBytes:   0,
		},
		{
			name:    "exact capacity",
			maxSize: 1000,
			entries: []*MemoryEntry{
				{Name: "blob1", Data: make([]byte, 1000), CreatedAt: time.Now()},
			},
			expectedAdded:   []bool{true},
			expectedEntries: 1,
			expectedBytes:   1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewBlobMemoryCache(
				BlobMemoryCacheConfig{MaxSize: tt.maxSize},
				tally.NoopScope,
			)

			for i, entry := range tt.entries {
				// Try to reserve space first
				reserved := cache.TryReserve(entry.Size())

				var added bool
				if reserved {
					// Add the entry
					added = cache.Add(entry)

					if !added {
						// Entry already existed, release the reservation
						cache.ReleaseReservation(entry.Size())
					}
				} else {
					// Reservation failed, cannot add
					added = false
				}

				assert.Equal(t, tt.expectedAdded[i], added)
			}

			assert.Equal(t, tt.expectedEntries, cache.NumEntries())
			assert.Equal(t, tt.expectedBytes, cache.TotalBytes())
		})
	}
}

func TestBlobMemoryCache_Add_MultipleBlobs(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 2000},
		tally.NoopScope,
	)

	// Add multiple blobs
	for i := 0; i < 3; i++ {
		entry := &MemoryEntry{
			Name:      fmt.Sprintf("blob%d", i),
			Data:      make([]byte, 500),
			CreatedAt: time.Now(),
		}

		reserved := cache.TryReserve(entry.Size())
		require.True(t, reserved)

		cache.Add(entry)
	}

	assert.Equal(t, uint64(1500), cache.TotalBytes())
	assert.Equal(t, 3, cache.NumEntries())

	// Try to add one more that exceeds capacity
	entry := &MemoryEntry{
		Name:      "blob4",
		Data:      make([]byte, 600),
		CreatedAt: time.Now(),
	}

	reserved := cache.TryReserve(entry.Size())
	assert.False(t, reserved, "Should fail to reserve when total would exceed capacity")
	assert.Equal(t, uint64(1500), cache.TotalBytes(), "Size should not change")
	assert.Equal(t, 3, cache.NumEntries())
}

func TestBlobMemoryCache_Get(t *testing.T) {
	tests := []struct {
		name             string
		setupEntry       *MemoryEntry
		lookupKey        string
		expectFound      bool
		expectedName     string
		expectedDataSize int
	}{
		{
			name: "hit",
			setupEntry: &MemoryEntry{
				Name:      "blob1",
				Data:      []byte("test data"),
				CreatedAt: time.Now(),
			},
			lookupKey:        "blob1",
			expectFound:      true,
			expectedName:     "blob1",
			expectedDataSize: 9,
		},
		{
			name:        "miss",
			setupEntry:  nil,
			lookupKey:   "nonexistent",
			expectFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewBlobMemoryCache(
				BlobMemoryCacheConfig{MaxSize: 1000},
				tally.NoopScope,
			)

			if tt.setupEntry != nil {
				reserved := cache.TryReserve(tt.setupEntry.Size())
				require.True(t, reserved)
				cache.Add(tt.setupEntry)
			}

			result := cache.Get(tt.lookupKey)

			if tt.expectFound {
				require.NotNil(t, result)
				assert.Equal(t, tt.expectedName, result.Name)
				assert.Equal(t, uint64(tt.expectedDataSize), result.Size())
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestBlobMemoryCache_Get_WithMetaInfo(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 1000},
		tally.NoopScope,
	)

	digest := core.DigestFixture()
	metaInfo := core.MetaInfoFixture()
	entry := &MemoryEntry{
		Name:      digest.Hex(),
		Data:      []byte("test data"),
		MetaInfo:  metaInfo,
		CreatedAt: time.Now(),
	}

	reserved := cache.TryReserve(entry.Size())
	require.True(t, reserved)
	cache.Add(entry)

	result := cache.Get(digest.Hex())

	require.NotNil(t, result)
	assert.Equal(t, metaInfo, result.MetaInfo)
}

func TestBlobMemoryCache_Remove(t *testing.T) {
	tests := []struct {
		name          string
		setupEntry    *MemoryEntry
		removeKey     string
		expectedSize  int
		expectedBytes uint64
		shouldExist   bool
	}{
		{
			name: "existing entry",
			setupEntry: &MemoryEntry{
				Name:      "blob1",
				Data:      make([]byte, 500),
				CreatedAt: time.Now(),
			},
			removeKey:     "blob1",
			expectedSize:  0,
			expectedBytes: 0,
			shouldExist:   false,
		},
		{
			name:          "non-existent entry",
			setupEntry:    nil,
			removeKey:     "nonexistent",
			expectedSize:  0,
			expectedBytes: 0,
			shouldExist:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewBlobMemoryCache(
				BlobMemoryCacheConfig{MaxSize: 1000},
				tally.NoopScope,
			)

			if tt.setupEntry != nil {
				reserved := cache.TryReserve(tt.setupEntry.Size())
				require.True(t, reserved)
				cache.Add(tt.setupEntry)
			}

			cache.Remove(tt.removeKey)

			if tt.shouldExist {
				assert.NotNil(t, cache.Get(tt.removeKey))
			} else {
				assert.Nil(t, cache.Get(tt.removeKey))
			}
			assert.Equal(t, tt.expectedBytes, cache.TotalBytes())
			assert.Equal(t, tt.expectedSize, cache.NumEntries())
		})
	}
}

func TestBlobMemoryCache_Remove_AllowsReAdd(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 1000},
		tally.NoopScope,
	)

	entry1 := &MemoryEntry{
		Name:      "blob1",
		Data:      make([]byte, 600),
		CreatedAt: time.Now(),
	}
	reserved := cache.TryReserve(entry1.Size())
	require.True(t, reserved)
	cache.Add(entry1)

	// Try to add another blob - should fail due to capacity
	entry2 := &MemoryEntry{
		Name:      "blob2",
		Data:      make([]byte, 500),
		CreatedAt: time.Now(),
	}
	reserved = cache.TryReserve(entry2.Size())
	assert.False(t, reserved)

	// Remove first blob
	cache.Remove("blob1")

	// Now second blob should fit
	reserved = cache.TryReserve(entry2.Size())
	require.True(t, reserved)
	cache.Add(entry2)
	assert.Equal(t, uint64(500), cache.TotalBytes())
}

func TestBlobMemoryCache_ConcurrentAdd(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 100000},
		tally.NoopScope,
	)

	var wg sync.WaitGroup
	numGoroutines := 50
	entriesPerGoroutine := 10

	// Concurrent adds
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < entriesPerGoroutine; j++ {
				entry := &MemoryEntry{
					Name:      fmt.Sprintf("blob-%d-%d", id, j),
					Data:      make([]byte, 100),
					CreatedAt: time.Now(),
				}
				reserved := cache.TryReserve(entry.Size())
				if reserved {
					cache.Add(entry)
				}
			}
		}(i)
	}

	wg.Wait()

	// All entries should be added (no race conditions)
	assert.Equal(t, numGoroutines*entriesPerGoroutine, cache.NumEntries())
	assert.Equal(t, uint64(numGoroutines*entriesPerGoroutine*100), cache.TotalBytes())
}

func TestBlobMemoryCache_ConcurrentReadWrite(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 10000},
		tally.NoopScope,
	)

	// Pre-populate cache
	for i := 0; i < 10; i++ {
		entry := &MemoryEntry{
			Name:      fmt.Sprintf("blob%d", i),
			Data:      make([]byte, 100),
			CreatedAt: time.Now(),
		}
		reserved := cache.TryReserve(entry.Size())
		require.True(t, reserved)
		cache.Add(entry)
	}

	var wg sync.WaitGroup

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				cache.Get(fmt.Sprintf("blob%d", id))
			}
		}(i)
	}

	// Concurrent writes
	for i := 10; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			entry := &MemoryEntry{
				Name:      fmt.Sprintf("blob%d", id),
				Data:      make([]byte, 100),
				CreatedAt: time.Now(),
			}
			reserved := cache.TryReserve(entry.Size())
			if reserved {
				cache.Add(entry)
			}
		}(i)
	}

	// Concurrent removes
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cache.Remove(fmt.Sprintf("blob%d", id))
		}(i)
	}

	wg.Wait()
	// No race conditions, test passes
}

func TestBlobMemoryCache_SizeAndTotalBytes(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 5000},
		tally.NoopScope,
	)

	entries := []*MemoryEntry{
		{Name: "blob1", Data: make([]byte, 100), CreatedAt: time.Now()},
		{Name: "blob2", Data: make([]byte, 200), CreatedAt: time.Now()},
		{Name: "blob3", Data: make([]byte, 300), CreatedAt: time.Now()},
	}

	for _, entry := range entries {
		reserved := cache.TryReserve(entry.Size())
		require.True(t, reserved)
		cache.Add(entry)
	}

	assert.Equal(t, 3, cache.NumEntries())
	assert.Equal(t, uint64(600), cache.TotalBytes())

	cache.Remove("blob2")

	assert.Equal(t, 2, cache.NumEntries())
	assert.Equal(t, uint64(400), cache.TotalBytes())
}

func TestBlobMemoryCache_TryReserve_ReserveFailedCounter(t *testing.T) {
	tests := []struct {
		name              string
		maxSize           uint64
		firstReserveSize  uint64
		secondReserveSize uint64
		expectMetrics     bool
	}{
		{
			name:              "emits reserve_failed when capacity exceeded",
			maxSize:           1000,
			firstReserveSize:  800,
			secondReserveSize: 300,
			expectMetrics:     true,
		},
		{
			name:              "does not emit reserve_failed when within capacity",
			maxSize:           1000,
			firstReserveSize:  500,
			secondReserveSize: 400,
			expectMetrics:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testScope := tally.NewTestScope("test", nil)
			cache := NewBlobMemoryCache(
				BlobMemoryCacheConfig{MaxSize: tt.maxSize},
				testScope,
			)

			cache.TryReserve(tt.firstReserveSize)
			cache.TryReserve(tt.secondReserveSize)

			snapshot := testScope.Snapshot()
			counters := snapshot.Counters()
			reserveFailedCounter, exists := counters["test.blob_memory_cache.reserve_failure+"]
			if !tt.expectMetrics {
				assert.False(t, exists)
			} else {
				require.True(t, exists, "reserve_failed counter should exist when failures occur")
				assert.Equal(t, int64(1), reserveFailedCounter.Value())
			}
		})
	}
}
