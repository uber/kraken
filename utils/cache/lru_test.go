// Copyright (c) 2016-2019 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLRUCacheConfig_ApplyDefaults(t *testing.T) {
	tests := []struct {
		name     string
		input    LRUCacheConfig
		expected LRUCacheConfig
	}{
		{
			name:     "zero values get defaults",
			input:    LRUCacheConfig{},
			expected: LRUCacheConfig{Size: 300, TTL: 5 * time.Minute},
		},
		{
			name:     "positive values are preserved",
			input:    LRUCacheConfig{Size: 500, TTL: 10 * time.Minute},
			expected: LRUCacheConfig{Size: 500, TTL: 10 * time.Minute},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.input.applyDefaults()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestLRUCache_Basic(t *testing.T) {
	cache := NewLRUCache(LRUCacheConfig{Size: 3, TTL: time.Hour})

	// Initially empty
	require.Equal(t, 0, cache.Size())
	require.False(t, cache.Has("key1"))

	// Add entries
	cache.Add("key1")
	require.True(t, cache.Has("key1"))
	require.Equal(t, 1, cache.Size())

	cache.Add("key2")
	cache.Add("key3")
	require.Equal(t, 3, cache.Size())

	// All keys should exist
	require.True(t, cache.Has("key1"))
	require.True(t, cache.Has("key2"))
	require.True(t, cache.Has("key3"))
}

func TestLRUCache_SizeLimit(t *testing.T) {
	cache := NewLRUCache(LRUCacheConfig{Size: 2, TTL: time.Hour})

	// Fill cache to capacity
	cache.Add("key1")
	cache.Add("key2")
	require.Equal(t, 2, cache.Size())

	// Adding third key should evict oldest (key1)
	cache.Add("key3")
	require.Equal(t, 2, cache.Size())
	require.False(t, cache.Has("key1")) // Evicted
	require.True(t, cache.Has("key2"))
	require.True(t, cache.Has("key3"))
}

func TestLRUCache_LRUOrdering(t *testing.T) {
	cache := NewLRUCache(LRUCacheConfig{Size: 2, TTL: time.Hour})

	// Add two keys
	cache.Add("key1")
	cache.Add("key2")

	// Access key1 (should move it to end of LRU)
	cache.Add("key1") // Re-adding existing key updates its position

	// Add key3 - should evict key2 (not key1, since key1 was recently accessed)
	cache.Add("key3")
	require.True(t, cache.Has("key1"))  // Still present
	require.False(t, cache.Has("key2")) // Evicted
	require.True(t, cache.Has("key3"))  // New entry
}

func TestLRUCache_TTL(t *testing.T) {
	cache := NewLRUCache(LRUCacheConfig{Size: 10, TTL: 50 * time.Millisecond})

	cache.Add("key1")
	require.True(t, cache.Has("key1"))

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)
	require.False(t, cache.Has("key1")) // Should be expired
}

func TestLRUCache_Delete(t *testing.T) {
	cache := NewLRUCache(LRUCacheConfig{Size: 10, TTL: time.Hour})

	cache.Add("key1")
	cache.Add("key2")
	require.True(t, cache.Has("key1"))
	require.True(t, cache.Has("key2"))
	require.Equal(t, 2, cache.Size())

	cache.Delete("key1")
	require.False(t, cache.Has("key1"))
	require.True(t, cache.Has("key2"))
	require.Equal(t, 1, cache.Size())

	// Delete non-existent key should be safe
	cache.Delete("nonexistent")
	require.Equal(t, 1, cache.Size())
}

func TestLRUCache_Clear(t *testing.T) {
	cache := NewLRUCache(LRUCacheConfig{Size: 10, TTL: time.Hour})

	cache.Add("key1")
	cache.Add("key2")
	cache.Add("key3")
	require.Equal(t, 3, cache.Size())

	cache.Clear()
	require.Equal(t, 0, cache.Size())
	require.False(t, cache.Has("key1"))
	require.False(t, cache.Has("key2"))
	require.False(t, cache.Has("key3"))
}

func TestLRUCache_ConcurrentAccess(t *testing.T) {
	cache := NewLRUCache(LRUCacheConfig{Size: 100, TTL: time.Hour})

	// Test concurrent reads and writes
	done := make(chan bool, 10)

	// Start multiple goroutines doing concurrent operations
	for i := 0; i < 5; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := "key" + string(rune('0'+id))
				cache.Add(key)
				cache.Has(key)
			}
			done <- true
		}(i)
	}

	for i := 0; i < 5; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := "key" + string(rune('0'+id))
				cache.Has(key)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not crash and should have some entries
	require.True(t, cache.Size() > 0)
}

// Benchmark tests for measuring LRU cache performance

func BenchmarkLRUCache_Add(b *testing.B) {
	cache := NewLRUCache(LRUCacheConfig{Size: 1000, TTL: time.Hour})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Add(fmt.Sprintf("key%d", i))
	}
}

func BenchmarkLRUCache_Has_Hit(b *testing.B) {
	cache := NewLRUCache(LRUCacheConfig{Size: 1000, TTL: time.Hour})

	// Pre-populate cache
	for i := 0; i < 500; i++ {
		cache.Add(fmt.Sprintf("key%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Has(fmt.Sprintf("key%d", i%500))
	}
}

func BenchmarkLRUCache_Has_Miss(b *testing.B) {
	cache := NewLRUCache(LRUCacheConfig{Size: 1000, TTL: time.Hour})

	// Pre-populate cache
	for i := 0; i < 500; i++ {
		cache.Add(fmt.Sprintf("key%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Has(fmt.Sprintf("miss%d", i))
	}
}

func BenchmarkLRUCache_Mixed_ReadHeavy(b *testing.B) {
	cache := NewLRUCache(LRUCacheConfig{Size: 1000, TTL: time.Hour})

	// Pre-populate cache
	for i := 0; i < 100; i++ {
		cache.Add(fmt.Sprintf("key%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			cache.Add(fmt.Sprintf("key%d", i%1000))
		} else {
			cache.Has(fmt.Sprintf("key%d", i%100))
		}
	}
}

func BenchmarkLRUCache_ConcurrentAccess(b *testing.B) {
	cache := NewLRUCache(LRUCacheConfig{Size: 1000, TTL: time.Hour})

	// Pre-populate cache
	for i := 0; i < 100; i++ {
		cache.Add(fmt.Sprintf("key%d", i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i%100)
			if i%3 == 0 {
				cache.Add(key)
			} else {
				cache.Has(key)
			}
			i++
		}
	})
}

func BenchmarkLRUCache_EvictionPressure(b *testing.B) {
	cache := NewLRUCache(LRUCacheConfig{Size: 50, TTL: time.Hour}) // Small cache to force evictions

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Add(fmt.Sprintf("key%d", i))
	}
}
