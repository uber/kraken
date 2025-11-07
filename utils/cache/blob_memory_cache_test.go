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
		maxSize         int64
		entries         []*MemoryEntry
		expectedAdded   []bool
		expectedEntries int
		expectedBytes   int64
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
			expectedAdded:   []bool{true, true},
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
				added, err := cache.Add(entry)
				require.NoError(t, err)
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

		added, err := cache.Add(entry)
		require.NoError(t, err)
		assert.True(t, added)
	}

	assert.Equal(t, int64(1500), cache.TotalBytes())
	assert.Equal(t, 3, cache.NumEntries())

	// Try to add one more that exceeds capacity
	entry := &MemoryEntry{
		Name:      "blob4",
		Data:      make([]byte, 600),
		CreatedAt: time.Now(),
	}

	added, err := cache.Add(entry)
	require.NoError(t, err)
	assert.False(t, added, "Should fail when total would exceed capacity")
	assert.Equal(t, int64(1500), cache.TotalBytes(), "Size should not change")
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
				_, err := cache.Add(tt.setupEntry)
				require.NoError(t, err)
			}

			result := cache.Get(tt.lookupKey)

			if tt.expectFound {
				require.NotNil(t, result)
				assert.Equal(t, tt.expectedName, result.Name)
				assert.Equal(t, int64(tt.expectedDataSize), result.Size())
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
	_, err := cache.Add(entry)
	require.NoError(t, err)

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
		expectedBytes int64
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
				_, err := cache.Add(tt.setupEntry)
				require.NoError(t, err)
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
	added, err := cache.Add(entry1)
	require.NoError(t, err)
	require.True(t, added)

	// Try to add another blob - should fail due to capacity
	entry2 := &MemoryEntry{
		Name:      "blob2",
		Data:      make([]byte, 500),
		CreatedAt: time.Now(),
	}
	added, err = cache.Add(entry2)
	require.NoError(t, err)
	assert.False(t, added)

	// Remove first blob
	cache.Remove("blob1")

	// Now second blob should fit
	added, err = cache.Add(entry2)
	require.NoError(t, err)
	assert.True(t, added)
	assert.Equal(t, int64(500), cache.TotalBytes())
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
				_, err := cache.Add(entry)
				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// All entries should be added (no race conditions)
	assert.Equal(t, numGoroutines*entriesPerGoroutine, cache.NumEntries())
	assert.Equal(t, int64(numGoroutines*entriesPerGoroutine*100), cache.TotalBytes())
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
		_, err := cache.Add(entry)
		require.NoError(t, err)
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
			_, err := cache.Add(entry)
			require.NoError(t, err)
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
		_, err := cache.Add(entry)
		require.NoError(t, err)
	}

	assert.Equal(t, 3, cache.NumEntries())
	assert.Equal(t, int64(600), cache.TotalBytes())

	cache.Remove("blob2")

	assert.Equal(t, 2, cache.NumEntries())
	assert.Equal(t, int64(400), cache.TotalBytes())
}
