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

func TestBlobMemoryCache_Add_Success(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 1000},
		tally.NoopScope,
	)

	entry := &MemoryEntry{
		Name:      "blob1",
		Data:      make([]byte, 500),
		Size:      500,
		CreatedAt: time.Now(),
	}

	added, err := cache.Add(entry)

	require.NoError(t, err)
	assert.True(t, added)
	assert.Equal(t, int64(500), cache.TotalBytes())
	assert.Equal(t, 1, cache.Size())
}

func TestBlobMemoryCache_Add_InsufficientSpace(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 1000},
		tally.NoopScope,
	)

	entry := &MemoryEntry{
		Name:      "blob1",
		Data:      make([]byte, 1500),
		Size:      1500,
		CreatedAt: time.Now(),
	}

	added, err := cache.Add(entry)

	require.NoError(t, err)
	assert.False(t, added, "Should return false when insufficient space")
	assert.Equal(t, int64(0), cache.TotalBytes())
	assert.Equal(t, 0, cache.Size())
}

func TestBlobMemoryCache_Add_AlreadyExists(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 2000},
		tally.NoopScope,
	)

	entry1 := &MemoryEntry{
		Name:      "blob1",
		Data:      make([]byte, 500),
		Size:      500,
		CreatedAt: time.Now(),
	}

	// Add first time
	added, err := cache.Add(entry1)
	require.NoError(t, err)
	require.True(t, added)

	// Add again - should return true without adding duplicate
	entry2 := &MemoryEntry{
		Name:      "blob1",
		Data:      make([]byte, 500),
		Size:      500,
		CreatedAt: time.Now(),
	}

	added, err = cache.Add(entry2)
	require.NoError(t, err)
	assert.True(t, added, "Should return true when entry already exists")
	assert.Equal(t, int64(500), cache.TotalBytes(), "Size should not change")
	assert.Equal(t, 1, cache.Size(), "Should still have only 1 entry")
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
			Size:      500,
			CreatedAt: time.Now(),
		}

		added, err := cache.Add(entry)
		require.NoError(t, err)
		assert.True(t, added)
	}

	assert.Equal(t, int64(1500), cache.TotalBytes())
	assert.Equal(t, 3, cache.Size())

	// Try to add one more that exceeds capacity
	entry := &MemoryEntry{
		Name:      "blob4",
		Data:      make([]byte, 600),
		Size:      600,
		CreatedAt: time.Now(),
	}

	added, err := cache.Add(entry)
	require.NoError(t, err)
	assert.False(t, added, "Should fail when total would exceed capacity")
	assert.Equal(t, int64(1500), cache.TotalBytes(), "Size should not change")
	assert.Equal(t, 3, cache.Size())
}

func TestBlobMemoryCache_Get_Hit(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 1000},
		tally.NoopScope,
	)

	data := []byte("test data")
	entry := &MemoryEntry{
		Name:      "blob1",
		Data:      data,
		Size:      int64(len(data)),
		CreatedAt: time.Now(),
	}
	_, err := cache.Add(entry)
	require.NoError(t, err)

	result := cache.Get("blob1")

	require.NotNil(t, result)
	assert.Equal(t, "blob1", result.Name)
	assert.Equal(t, data, result.Data)
	assert.Equal(t, int64(len(data)), result.Size)
}

func TestBlobMemoryCache_Get_Miss(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 1000},
		tally.NoopScope,
	)

	result := cache.Get("nonexistent")

	assert.Nil(t, result)
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
		Size:      9,
		CreatedAt: time.Now(),
	}
	_, err := cache.Add(entry)
	require.NoError(t, err)

	result := cache.Get(digest.Hex())

	require.NotNil(t, result)
	assert.Equal(t, metaInfo, result.MetaInfo)
}

func TestBlobMemoryCache_Remove(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 1000},
		tally.NoopScope,
	)

	entry := &MemoryEntry{
		Name:      "blob1",
		Data:      make([]byte, 500),
		Size:      500,
		CreatedAt: time.Now(),
	}
	_, err := cache.Add(entry)
	require.NoError(t, err)

	cache.Remove("blob1")

	assert.Nil(t, cache.Get("blob1"))
	assert.Equal(t, int64(0), cache.TotalBytes())
	assert.Equal(t, 0, cache.Size())
}

func TestBlobMemoryCache_Remove_NonExistent(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 1000},
		tally.NoopScope,
	)

	// Should not panic or error
	cache.Remove("nonexistent")

	assert.Equal(t, int64(0), cache.TotalBytes())
	assert.Equal(t, 0, cache.Size())
}

func TestBlobMemoryCache_Remove_AllowsReAdd(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 1000},
		tally.NoopScope,
	)

	entry1 := &MemoryEntry{
		Name:      "blob1",
		Data:      make([]byte, 600),
		Size:      600,
		CreatedAt: time.Now(),
	}
	added, err := cache.Add(entry1)
	require.NoError(t, err)
	require.True(t, added)

	// Try to add another blob - should fail due to capacity
	entry2 := &MemoryEntry{
		Name:      "blob2",
		Data:      make([]byte, 500),
		Size:      500,
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
					Size:      100,
					CreatedAt: time.Now(),
				}
				_, err := cache.Add(entry)
				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// All entries should be added (no race conditions)
	assert.Equal(t, numGoroutines*entriesPerGoroutine, cache.Size())
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
			Size:      100,
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
				Size:      100,
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
		{Name: "blob1", Data: make([]byte, 100), Size: 100},
		{Name: "blob2", Data: make([]byte, 200), Size: 200},
		{Name: "blob3", Data: make([]byte, 300), Size: 300},
	}

	for _, entry := range entries {
		_, err := cache.Add(entry)
		require.NoError(t, err)
	}

	assert.Equal(t, 3, cache.Size())
	assert.Equal(t, int64(600), cache.TotalBytes())

	cache.Remove("blob2")

	assert.Equal(t, 2, cache.Size())
	assert.Equal(t, int64(400), cache.TotalBytes())
}

func TestBlobMemoryCache_ZeroMaxSize(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 0},
		tally.NoopScope,
	)

	entry := &MemoryEntry{
		Name:      "blob1",
		Data:      make([]byte, 100),
		Size:      100,
		CreatedAt: time.Now(),
	}

	added, err := cache.Add(entry)
	require.NoError(t, err)
	assert.False(t, added, "Should not add when max size is 0")
}

func TestBlobMemoryCache_ExactCapacity(t *testing.T) {
	cache := NewBlobMemoryCache(
		BlobMemoryCacheConfig{MaxSize: 1000},
		tally.NoopScope,
	)

	entry := &MemoryEntry{
		Name:      "blob1",
		Data:      make([]byte, 1000),
		Size:      1000,
		CreatedAt: time.Now(),
	}

	added, err := cache.Add(entry)
	require.NoError(t, err)
	assert.True(t, added, "Should add when exactly at capacity")
	assert.Equal(t, int64(1000), cache.TotalBytes())
}
