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
package store

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/cache"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/testutil"
)

func TestCAStoreInitVolumes(t *testing.T) {
	require := require.New(t)

	config, cleanup := CAStoreConfigFixture()
	defer cleanup()

	volume1, err := os.MkdirTemp("/tmp", "volume")
	require.NoError(err)
	t.Cleanup(func() {
		require.NoError(os.RemoveAll(volume1))
	})

	volume2, err := os.MkdirTemp("/tmp", "volume")
	require.NoError(err)
	t.Cleanup(func() {
		require.NoError(os.RemoveAll(volume2))
	})

	volume3, err := os.MkdirTemp("/tmp", "volume")
	require.NoError(err)
	t.Cleanup(func() {
		require.NoError(os.RemoveAll(volume3))
	})

	config.Volumes = []Volume{
		{Location: volume1, Weight: 100},
		{Location: volume2, Weight: 100},
		{Location: volume3, Weight: 100},
	}

	_, err = NewCAStore(config, tally.NoopScope)
	require.NoError(err)

	v1Files, err := os.ReadDir(path.Join(volume1, path.Base(config.CacheDir)))
	require.NoError(err)
	v2Files, err := os.ReadDir(path.Join(volume2, path.Base(config.CacheDir)))
	require.NoError(err)
	v3Files, err := os.ReadDir(path.Join(volume3, path.Base(config.CacheDir)))
	require.NoError(err)
	n1 := len(v1Files)
	n2 := len(v2Files)
	n3 := len(v3Files)

	// There should be 256 symlinks total, evenly distributed across the volumes.
	require.Equal(256, (n1 + n2 + n3))
	require.True(float32(n1)/256 > float32(0.25), "%d/256 should be >0.25", n1)
	require.True(float32(n2)/256 > float32(0.25), "%d/256 should be >0.25", n2)
	require.True(float32(n3)/256 > float32(0.25), "%d/256 should be >0.25", n3)
}

func TestCAStoreInitVolumesAfterChangingVolumes(t *testing.T) {
	require := require.New(t)

	config, cleanup := CAStoreConfigFixture()
	defer cleanup()

	volume1, err := os.MkdirTemp("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume1)

	volume2, err := os.MkdirTemp("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume2)

	volume3, err := os.MkdirTemp("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume3)

	config.Volumes = []Volume{
		{Location: volume1, Weight: 100},
		{Location: volume2, Weight: 100},
		{Location: volume3, Weight: 100},
	}

	_, err = NewCAStore(config, tally.NoopScope)
	require.NoError(err)

	// Add one more volume, recreate file store.

	volume4, err := os.MkdirTemp("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume3)

	config.Volumes = append(config.Volumes, Volume{Location: volume4, Weight: 100})

	_, err = NewCAStore(config, tally.NoopScope)
	require.NoError(err)

	var n1, n2, n3, n4 int
	links, err := os.ReadDir(config.CacheDir)
	require.NoError(err)
	for _, link := range links {
		source, err := os.Readlink(path.Join(config.CacheDir, link.Name()))
		require.NoError(err)
		if strings.HasPrefix(source, volume1) {
			n1++
		}
		if strings.HasPrefix(source, volume2) {
			n2++
		}
		if strings.HasPrefix(source, volume3) {
			n3++
		}
		if strings.HasPrefix(source, volume4) {
			n4++
		}
	}

	// Symlinks should be recreated.
	require.Equal(256, (n1 + n2 + n3 + n4))
	require.True(float32(n1)/256 > float32(0.15))
	require.True(float32(n2)/256 > float32(0.15))
	require.True(float32(n3)/256 > float32(0.15))
	require.True(float32(n4)/256 > float32(0.15))
}

func TestCAStoreCreateUploadFileAndMoveToCache(t *testing.T) {
	require := require.New(t)

	config, cleanup := CAStoreConfigFixture()
	defer cleanup()

	s, err := NewCAStore(config, tally.NoopScope)
	require.NoError(err)

	src := core.DigestFixture().Hex()

	require.NoError(s.CreateUploadFile(src, 100))
	_, err = os.Stat(path.Join(config.UploadDir, src))
	require.NoError(err)

	f, err := s.uploadStore.newFileOp().GetFileReader(src, 0 /* readPartSize */)
	require.NoError(err)
	defer f.Close()
	digester := core.NewDigester()
	digest, err := digester.FromReader(f)
	require.NoError(err)
	dst := digest.Hex()

	err = s.MoveUploadFileToCache(src, dst)
	require.NoError(err)
	_, err = os.Stat(path.Join(config.UploadDir, src[:2], src[2:4], src))
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(config.CacheDir, dst[:2], dst[2:4], dst))
	require.NoError(err)
}

func TestCAStoreCreateUploadFileAndMoveToCacheFailure(t *testing.T) {
	require := require.New(t)

	config, cleanup := CAStoreConfigFixture()
	defer cleanup()

	s, err := NewCAStore(config, tally.NoopScope)
	require.NoError(err)

	src := core.DigestFixture().Hex()

	require.NoError(s.CreateUploadFile(src, 100))
	_, err = os.Stat(path.Join(config.UploadDir, src))
	require.NoError(err)

	f, err := s.uploadStore.newFileOp().GetFileReader(src, 0 /* readPartSize */)
	require.NoError(err)
	defer f.Close()
	digester := core.NewDigester()
	digest, err := digester.FromReader(f)
	require.NoError(err)

	dst := core.DigestFixture().Hex()
	err = s.MoveUploadFileToCache(src, dst)
	require.EqualError(err, fmt.Sprintf("verify digest: computed digest sha256:%s doesn't match expected value sha256:%s", digest.Hex(), dst))
	_, err = os.Stat(path.Join(config.UploadDir, src[:2], src[2:4], src))
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(config.CacheDir, dst[:2], dst[2:4], dst))
	require.True(os.IsNotExist(err))
}

func TestCAStoreCreateCacheFile(t *testing.T) {
	require := require.New(t)

	s, cleanup := CAStoreFixture()
	defer cleanup()

	s1 := "buffer"
	computedDigest, err := core.NewDigester().FromBytes([]byte(s1))
	require.NoError(err)
	r1 := strings.NewReader(s1)

	err = s.CreateCacheFile(computedDigest.Hex(), r1)
	require.NoError(err)
	r2, err := s.GetCacheFileReader(computedDigest.Hex())
	require.NoError(err)
	b2, err := io.ReadAll(r2)
	require.Equal(s1, string(b2))
}
func TestCAStoreConfig_WithMemoryCache(t *testing.T) {
	require := require.New(t)

	config, cleanup := CAStoreConfigFixture()
	defer cleanup()

	require.Equal(uint64(0), config.MemoryCache.MaxSize)
	require.Equal(0, config.MemoryCache.DrainWorkers)
	require.Equal(0, config.MemoryCache.DrainMaxRetries)
	require.Equal(time.Duration(0), config.MemoryCache.TTLInterval)

	config = config.applyDefaults()

	require.Equal(10, config.MemoryCache.DrainWorkers)
	require.Equal(3, config.MemoryCache.DrainMaxRetries)
	require.Equal(5*time.Minute, config.MemoryCache.TTL)
}

func TestCAStore_Init_MemoryCache(t *testing.T) {
	tests := []struct {
		name              string
		memoryCacheConfig MemoryCacheConfig
		expectMemCache    bool
		expectTTLWorker   bool
	}{
		{
			name: "with memory cache",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: true,
				MaxSize: 1024 * 1024,
				TTL:     time.Hour,
			},
			expectMemCache:  true,
			expectTTLWorker: true,
		},
		{
			name: "without memory cache",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: false,
			},
			expectMemCache:  false,
			expectTTLWorker: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, cleanup := CAStoreConfigFixture()
			defer cleanup()

			config.MemoryCache = tt.memoryCacheConfig

			s, err := NewCAStore(config, tally.NoopScope)
			require.NoError(t, err)
			defer s.Close()

			if tt.expectMemCache {
				require.NotNil(t, s.memCache)
			} else {
				require.Nil(t, s.memCache)
			}

			if tt.expectTTLWorker {
				require.NotNil(t, s.ttlStopChan)
			} else {
				require.Nil(t, s.ttlStopChan)
			}
		})
	}
}

func TestCAStore_TTLCleanup(t *testing.T) {
	tests := []struct {
		name        string
		ttl         time.Duration
		entryAge    time.Duration
		shouldExist bool
	}{
		{
			name:        "removes expired entries",
			ttl:         time.Hour,
			entryAge:    2 * time.Hour,
			shouldExist: false,
		},
		{
			name:        "does not remove fresh entries",
			ttl:         time.Hour,
			entryAge:    30 * time.Minute,
			shouldExist: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, cleanup := CAStoreConfigFixture()
			defer cleanup()

			config.MemoryCache = MemoryCacheConfig{
				Enabled: true,
				MaxSize: 1024 * 1024,
				TTL:     tt.ttl,
			}

			mockClock := clock.NewMock()
			s, err := newCAStore(config, tally.NoopScope, mockClock)
			require.NoError(t, err)
			defer s.Close()

			now := mockClock.Now()
			entry := &cache.MemoryEntry{
				Name:      "test",
				Data:      []byte("test data"),
				CreatedAt: now.Add(-tt.entryAge),
			}
			added := s.memCache.Add(entry)
			require.True(t, added)

			s.cleanupMemoryCacheExpiredEntries()

			result := s.memCache.Get("test")
			if tt.shouldExist {
				require.NotNil(t, result)
			} else {
				require.Nil(t, result)
			}
		})
	}
}

func TestCAStore_CloseWithTTLWorker(t *testing.T) {
	require := require.New(t)

	config, cleanup := CAStoreConfigFixture()
	defer cleanup()

	config.MemoryCache = MemoryCacheConfig{
		Enabled: true,
		MaxSize: 1024 * 1024,
		TTL:     time.Hour,
	}

	s, err := NewCAStore(config, tally.NoopScope)
	require.NoError(err)

	s.Close()
}

func TestCAStore_WriteBlobToCacheWithMetaInfo_Success(t *testing.T) {
	tests := []struct {
		name              string
		memoryCacheConfig MemoryCacheConfig
		testData          []byte
		expectInMemCache  bool
		expectOnDisk      bool
	}{
		{
			name: "memory cache success",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: true,
				MaxSize: 1024 * 1024,
				TTL:     time.Hour,
			},
			testData:         []byte("test data for memory cache"),
			expectInMemCache: true,
			expectOnDisk:     false,
		},
		{
			name: "cache full - fallback to disk",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: true,
				MaxSize: 100,
				TTL:     time.Hour,
			},
			testData:         make([]byte, 200),
			expectInMemCache: false,
			expectOnDisk:     true,
		},
		{
			name: "cache disabled - write to disk",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: false,
			},
			testData:         []byte("test data"),
			expectInMemCache: false,
			expectOnDisk:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, cleanup := CAStoreConfigFixture()
			defer cleanup()

			config.MemoryCache = tt.memoryCacheConfig

			// Initialize test data if needed
			if len(tt.testData) == 200 {
				for i := range tt.testData {
					tt.testData[i] = byte(i % 256)
				}
			}

			mockClock := clock.NewMock()
			s, err := newCAStore(config, tally.NoopScope, mockClock)
			require.NoError(t, err)
			defer s.Close()

			digest, err := core.NewDigester().FromBytes(tt.testData)
			require.NoError(t, err)
			name := digest.Hex()

			err = s.WriteBlobToCacheWithMetaInfo(name, uint64(len(tt.testData)),
				func(w FileReadWriter) error {
					_, err := w.Write(tt.testData)
					return err
				},
				256*1024,
			)

			require.NoError(t, err)

			if config.MemoryCache.Enabled {
				inMemCache := s.CheckInMemCache(name)
				require.Equal(t, tt.expectInMemCache, inMemCache)

				if tt.expectInMemCache {
					entry := s.memCache.Get(name)
					require.NotNil(t, entry, "Blob should be in memory cache")
					require.Equal(t, tt.testData, entry.Data)
					require.NotNil(t, entry.MetaInfo, "MetaInfo should be generated")
				}
			}

			if tt.expectOnDisk {
				r, err := s.GetCacheFileReader(name)
				require.NoError(t, err)
				defer closers.Close(r)
				readData, err := io.ReadAll(r)
				require.NoError(t, err)
				require.Equal(t, tt.testData, readData, "Blob should be on disk")
			}
		})
	}
}

func TestCAStore_WriteBlobToCacheWithMetaInfo_Errors(t *testing.T) {
	tests := []struct {
		name            string
		writeShouldFail bool
		expectError     bool
	}{
		{
			name:            "write error",
			writeShouldFail: true,
			expectError:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, cleanup := CAStoreConfigFixture()
			defer cleanup()

			config.MemoryCache = MemoryCacheConfig{
				Enabled: true,
				MaxSize: 1024 * 1024,
				TTL:     time.Hour,
			}

			mockClock := clock.NewMock()
			s, err := newCAStore(config, tally.NoopScope, mockClock)
			require.NoError(t, err)
			defer s.Close()

			testData := []byte("test data")
			digest, err := core.NewDigester().FromBytes(testData)
			require.NoError(t, err)
			name := digest.Hex()

			err = s.WriteBlobToCacheWithMetaInfo(name, uint64(len(testData)),
				func(w FileReadWriter) error {
					if tt.writeShouldFail {
						return fmt.Errorf("write error")
					}
					_, err := w.Write(testData)
					return err
				},
				256*1024,
			)

			if tt.expectError {
				require.Error(t, err)
				require.False(t, s.CheckInMemCache(name), "Blob should not be in memory cache on error")
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCAStore_WriteBlobToCacheWithMetaInfo_SequentialDuplicates(t *testing.T) {
	config, cleanup := CAStoreConfigFixture()
	defer cleanup()

	config.MemoryCache = MemoryCacheConfig{
		Enabled: true,
		MaxSize: 1024 * 1024,
		TTL:     time.Hour,
	}

	mockClock := clock.NewMock()
	s, err := newCAStore(config, tally.NoopScope, mockClock)
	require.NoError(t, err)
	defer s.Close()

	testData := []byte("duplicate test data")
	digest, err := core.NewDigester().FromBytes(testData)
	require.NoError(t, err)
	name := digest.Hex()

	// First write - should add to cache
	err = s.WriteBlobToCacheWithMetaInfo(name, uint64(len(testData)),
		func(w FileReadWriter) error {
			_, err := w.Write(testData)
			return err
		},
		256*1024,
	)
	require.NoError(t, err)
	require.True(t, s.CheckInMemCache(name), "First write should add to memory cache")

	entry := s.memCache.Get(name)
	require.NotNil(t, entry, "Blob should be in cache after first write")
	initialSize := s.memCache.TotalBytes()

	// Second write - should be a no-op (duplicate)
	err = s.WriteBlobToCacheWithMetaInfo(name, uint64(len(testData)),
		func(w FileReadWriter) error {
			_, err := w.Write(testData)
			return err
		},
		256*1024,
	)
	require.NoError(t, err)
	require.True(t, s.CheckInMemCache(name), "Entry should still be in memory cache")

	// Verify no reservation leak
	finalSize := s.memCache.TotalBytes()
	require.Equal(t, initialSize, finalSize, "Cache size should not change on duplicate write")
	require.NotNil(t, s.memCache.Get(name), "Entry should still be in cache")
}

func TestCAStore_WriteBlobToCacheWithMetaInfo_ReservationFails(t *testing.T) {
	tests := []struct {
		name                string
		cacheSize           uint64
		firstBlobSize       int
		secondBlobSize      int
		expectFirstInCache  bool
		expectSecondInCache bool
	}{
		{
			name:                "reservation fails when cache full",
			cacheSize:           1000,
			firstBlobSize:       600,
			secondBlobSize:      500,
			expectFirstInCache:  true,
			expectSecondInCache: false,
		},
		{
			name:                "reservation succeeds when space available",
			cacheSize:           2000,
			firstBlobSize:       600,
			secondBlobSize:      500,
			expectFirstInCache:  true,
			expectSecondInCache: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, cleanup := CAStoreConfigFixture()
			defer cleanup()

			config.MemoryCache = MemoryCacheConfig{
				Enabled: true,
				MaxSize: tt.cacheSize,
				TTL:     time.Hour,
			}

			mockClock := clock.NewMock()
			s, err := newCAStore(config, tally.NoopScope, mockClock)
			require.NoError(t, err)
			defer s.Close()

			// Add first blob
			data1 := make([]byte, tt.firstBlobSize)
			digest1, err := core.NewDigester().FromBytes(data1)
			require.NoError(t, err)

			err = s.WriteBlobToCacheWithMetaInfo(digest1.Hex(), uint64(tt.firstBlobSize),
				func(w FileReadWriter) error {
					_, err := w.Write(data1)
					return err
				},
				256*1024,
			)
			require.NoError(t, err)
			require.Equal(t, tt.expectFirstInCache, s.CheckInMemCache(digest1.Hex()))

			// Try to add second blob
			data2 := make([]byte, tt.secondBlobSize)
			for i := range data2 {
				data2[i] = byte(i % 256)
			}
			digest2, err := core.NewDigester().FromBytes(data2)
			require.NoError(t, err)

			err = s.WriteBlobToCacheWithMetaInfo(digest2.Hex(), uint64(tt.secondBlobSize),
				func(w FileReadWriter) error {
					_, err := w.Write(data2)
					return err
				},
				256*1024,
			)

			require.NoError(t, err)
			inMemCache := s.CheckInMemCache(digest2.Hex())
			require.Equal(t, tt.expectSecondInCache, inMemCache)

			if tt.expectSecondInCache {
				entry := s.memCache.Get(digest2.Hex())
				require.NotNil(t, entry, "Second blob should be in cache")
			} else {
				// Only verify on disk when not in cache
				r, err := s.GetCacheFileReader(digest2.Hex())
				require.NoError(t, err)
				defer closers.Close(r)
				readData, err := io.ReadAll(r)
				require.NoError(t, err)
				require.Equal(t, data2, readData, "Second blob should be on disk")
			}
		})
	}
}

func TestCAStore_WriteBlobToCacheWithMetaInfo_Multiple(t *testing.T) {
	tests := []struct {
		name             string
		entries          []string
		expectedQueueLen int
	}{
		{
			name:             "single entry",
			entries:          []string{"data1"},
			expectedQueueLen: 1,
		},
		{
			name:             "three entries",
			entries:          []string{"data1", "data2", "data3"},
			expectedQueueLen: 3,
		},
		{
			name:             "five entries",
			entries:          []string{"data1", "data2", "data3", "data4", "data5"},
			expectedQueueLen: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, cleanup := CAStoreConfigFixture()
			defer cleanup()

			config.MemoryCache = MemoryCacheConfig{
				Enabled: true,
				MaxSize: 10 * 1024 * 1024,
				TTL:     time.Hour,
			}

			mockClock := clock.NewMock()
			s, err := newCAStore(config, tally.NoopScope, mockClock)
			require.NoError(t, err)
			defer s.Close()

			for _, dataStr := range tt.entries {
				data := []byte(dataStr)
				digest, err := core.NewDigester().FromBytes(data)
				require.NoError(t, err)
				name := digest.Hex()

				err = s.WriteBlobToCacheWithMetaInfo(name, uint64(len(data)),
					func(w FileReadWriter) error {
						_, err := w.Write(data)
						return err
					},
					256*1024,
				)

				require.NoError(t, err)
				require.True(t, s.CheckInMemCache(name), "Blob %s should be added to memory cache", dataStr)
			}

			s.drain.mu.Lock()
			queueLen := s.drain.queue.Len()
			s.drain.mu.Unlock()
			require.Equal(t, tt.expectedQueueLen, queueLen)
		})
	}
}

func TestCAStore_ConcurrentDuplicateWrites(t *testing.T) {
	require := require.New(t)

	config, cleanup := CAStoreConfigFixture()
	defer cleanup()

	config.MemoryCache = MemoryCacheConfig{
		Enabled: true,
		MaxSize: 1024 * 1024,
		TTL:     time.Hour,
	}

	mockClock := clock.NewMock()
	s, err := newCAStore(config, tally.NoopScope, mockClock)
	require.NoError(err)
	defer s.Close()

	testData := []byte("concurrent test data")
	digest, err := core.NewDigester().FromBytes(testData)
	require.NoError(err)
	name := digest.Hex()

	initialSize := s.memCache.TotalBytes()

	// Write same blob concurrently 10 times
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(s.WriteBlobToCacheWithMetaInfo(name, uint64(len(testData)),
				func(w FileReadWriter) error {
					_, err := w.Write(testData)
					return err
				},
				256*1024,
			))
		}()
	}
	wg.Wait()

	// Verify no reservation leak - exactly one entry added
	expectedSize := initialSize + uint64(len(testData))
	require.Equal(expectedSize, s.memCache.TotalBytes(), "No reservation leak")
	require.True(s.CheckInMemCache(name), "Entry should be in cache")
}

func TestCAStore_DrainWorkers(t *testing.T) {
	tests := []struct {
		name            string
		drainWorkers    int
		drainMaxRetries int
		blobSize        uint64
		chunkSize       uint64
	}{
		{
			name:            "single worker drains blob",
			drainWorkers:    1,
			drainMaxRetries: 3,
			blobSize:        1024,
			chunkSize:       256,
		},
		{
			name:            "multiple workers drain blob",
			drainWorkers:    2,
			drainMaxRetries: 3,
			blobSize:        1024,
			chunkSize:       256,
		},
		{
			name:            "large blob",
			drainWorkers:    2,
			drainMaxRetries: 3,
			blobSize:        10 * 1024,
			chunkSize:       1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := CAStoreConfig{
				UploadDir: t.TempDir(),
				CacheDir:  t.TempDir(),
				MemoryCache: MemoryCacheConfig{
					Enabled:         true,
					MaxSize:         1024 * 1024,
					DrainWorkers:    tt.drainWorkers,
					DrainMaxRetries: tt.drainMaxRetries,
					TTL:             time.Hour,
				},
			}

			mockClock := clock.NewMock()
			cas, err := newCAStore(config, tally.NoopScope, mockClock)
			require.NoError(t, err)
			defer cas.Close()

			// Create a test blob
			blob := core.SizedBlobFixture(tt.blobSize, tt.chunkSize)

			// Write to cache (should go to memory)
			err = cas.WriteBlobToCacheWithMetaInfo(
				blob.Digest.Hex(),
				uint64(len(blob.Content)),
				func(w FileReadWriter) error {
					_, err := w.Write(blob.Content)
					return err
				},
				256*1024,
			)
			require.NoError(t, err)

			// Verify blob is in memory cache
			entry := cas.memCache.Get(blob.Digest.Hex())
			require.NotNil(t, entry, "Blob should be in memory cache")

			// Poll until blob is drained or timeout
			require.NoError(t, testutil.PollUntilTrue(500*time.Millisecond, func() bool {
				mockClock.Add(100 * time.Millisecond)
				return cas.memCache.Get(blob.Digest.Hex()) == nil
			}))
			// Verify blob is on disk
			reader, err := cas.GetCacheFileReader(blob.Digest.Hex())
			require.NoError(t, err)
			defer closers.Close(reader)

			diskData := make([]byte, len(blob.Content))
			_, err = reader.Read(diskData)
			require.NoError(t, err)
			require.Equal(t, blob.Content, diskData, "Disk data should match original")
		})
	}
}

func TestCAStore_GetCacheFileReader(t *testing.T) {
	tests := []struct {
		name              string
		memoryCacheConfig MemoryCacheConfig
		testData          string
		addToMemory       bool
		addToDisk         bool
	}{
		{
			name: "read from memory cache",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: true,
				MaxSize: 1024 * 1024,
				TTL:     time.Hour,
			},
			testData:    "test data from memory",
			addToMemory: true,
			addToDisk:   false,
		},
		{
			name: "read from disk when memory cache disabled",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: false,
			},
			testData:    "test data no memory cache",
			addToMemory: false,
			addToDisk:   true,
		},
		{
			name: "read from disk when memory cache enabled",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: true,
				MaxSize: 1024 * 1024,
				TTL:     time.Hour,
			},
			testData:    "test data from disk",
			addToMemory: false,
			addToDisk:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, cleanup := CAStoreConfigFixture()
			defer cleanup()

			config.MemoryCache = tt.memoryCacheConfig

			s, err := NewCAStore(config, tally.NoopScope)
			require.NoError(t, err)
			defer s.Close()

			if !tt.memoryCacheConfig.Enabled {
				require.Nil(t, s.memCache)
			}

			data := []byte(tt.testData)
			digest, err := core.NewDigester().FromBytes(data)
			require.NoError(t, err)
			name := digest.Hex()

			if tt.addToMemory {
				entry := &cache.MemoryEntry{
					Name:      name,
					Data:      data,
					CreatedAt: time.Now(),
				}
				added := s.memCache.Add(entry)
				require.True(t, added)
			}

			if tt.addToDisk {
				err = s.CreateCacheFile(name, bytes.NewReader(data))
				require.NoError(t, err)
			}

			// Read from cache
			reader, err := s.GetCacheFileReader(name)
			require.NoError(t, err)
			defer closers.Close(reader)

			readData, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, data, readData)
		})
	}
}

func TestCAStore_GetCacheFileMetadata(t *testing.T) {
	tests := []struct {
		name              string
		memoryCacheConfig MemoryCacheConfig
		testData          string
	}{
		{
			name: "read metadata from memory",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: true,
				MaxSize: 1024 * 1024,
				TTL:     time.Hour,
			},
			testData: "test data for metadata",
		},
		{
			name: "read metadata from disk when not in memory",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: true,
				MaxSize: 1024 * 1024,
				TTL:     time.Hour,
			},
			testData: "test data from disk",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, cleanup := CAStoreConfigFixture()
			defer cleanup()

			config.MemoryCache = tt.memoryCacheConfig

			s, err := NewCAStore(config, tally.NoopScope)
			require.NoError(t, err)
			defer s.Close()

			data := []byte(tt.testData)
			digest, err := core.NewDigester().FromBytes(data)
			require.NoError(t, err)
			name := digest.Hex()

			metaInfo, err := core.NewMetaInfo(digest, bytes.NewReader(data), 256*1024)
			require.NoError(t, err)

			if tt.name == "read metadata from memory" {
				// Add to memory cache
				entry := &cache.MemoryEntry{
					Name:      name,
					Data:      data,
					MetaInfo:  metaInfo,
					CreatedAt: time.Now(),
				}
				added := s.memCache.Add(entry)
				require.True(t, added)
			} else {
				// Write to disk only
				err = s.CreateCacheFile(name, bytes.NewReader(data))
				require.NoError(t, err)

				tm := metadata.NewTorrentMeta(metaInfo)
				_, err = s.SetCacheFileMetadata(name, tm)
				require.NoError(t, err)
			}

			// Read metadata
			tm := metadata.NewTorrentMeta(nil)
			err = s.GetCacheFileMetadata(name, tm)
			require.NoError(t, err)
			require.NotNil(t, tm.MetaInfo)
			require.Equal(t, digest, tm.MetaInfo.Digest())
		})
	}
}

func TestCAStore_GetCacheFileStat(t *testing.T) {
	tests := []struct {
		name              string
		memoryCacheConfig MemoryCacheConfig
		testData          string
	}{
		{
			name: "get stat from memory",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: true,
				MaxSize: 1024 * 1024,
				TTL:     time.Hour,
			},
			testData: "test data for stat",
		},
		{
			name: "get stat with large data",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: true,
				MaxSize: 1024 * 1024,
				TTL:     time.Hour,
			},
			testData: string(make([]byte, 10000)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, cleanup := CAStoreConfigFixture()
			defer cleanup()

			config.MemoryCache = tt.memoryCacheConfig

			s, err := NewCAStore(config, tally.NoopScope)
			require.NoError(t, err)
			defer s.Close()

			data := []byte(tt.testData)
			digest, err := core.NewDigester().FromBytes(data)
			require.NoError(t, err)
			name := digest.Hex()

			entry := &cache.MemoryEntry{
				Name:      name,
				Data:      data,
				CreatedAt: time.Now(),
			}
			added := s.memCache.Add(entry)
			require.True(t, added)

			// Get stat from memory
			fi, err := s.GetCacheFileStat(name)
			require.NoError(t, err)
			require.Equal(t, name, fi.Name())
			require.Equal(t, int64(len(data)), fi.Size())
			require.False(t, fi.IsDir())
		})
	}
}

func TestCAStore_ListCacheFiles(t *testing.T) {
	tests := []struct {
		name              string
		memoryCacheConfig MemoryCacheConfig
		memoryEntries     []string
		diskEntries       []string
	}{
		{
			name: "includes both memory and disk files",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: true,
				MaxSize: 1024 * 1024,
				TTL:     time.Hour,
			},
			memoryEntries: []string{"in memory only"},
			diskEntries:   []string{"on disk only"},
		},
		{
			name: "includes multiple memory entries",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: true,
				MaxSize: 1024 * 1024,
				TTL:     time.Hour,
			},
			memoryEntries: []string{"memory1", "memory2", "memory3"},
			diskEntries:   []string{"disk1"},
		},
		{
			name: "memory cache disabled lists only disk",
			memoryCacheConfig: MemoryCacheConfig{
				Enabled: false,
			},
			memoryEntries: []string{},
			diskEntries:   []string{"disk1", "disk2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, cleanup := CAStoreConfigFixture()
			defer cleanup()

			config.MemoryCache = tt.memoryCacheConfig

			s, err := NewCAStore(config, tally.NoopScope)
			require.NoError(t, err)
			defer s.Close()

			expectedFiles := make(map[string]bool)

			// Add entries to memory
			for _, dataStr := range tt.memoryEntries {
				data := []byte(dataStr)
				digest, err := core.NewDigester().FromBytes(data)
				require.NoError(t, err)
				name := digest.Hex()

				entry := &cache.MemoryEntry{
					Name:      name,
					Data:      data,
					CreatedAt: time.Now(),
				}
				added := s.memCache.Add(entry)
				require.True(t, added)

				expectedFiles[name] = true
			}

			// Add entries to disk
			for _, dataStr := range tt.diskEntries {
				data := []byte(dataStr)
				digest, err := core.NewDigester().FromBytes(data)
				require.NoError(t, err)
				name := digest.Hex()

				err = s.CreateCacheFile(name, bytes.NewReader(data))
				require.NoError(t, err)

				expectedFiles[name] = true
			}

			// List should include all expected files
			files, err := s.ListCacheFiles()
			require.NoError(t, err)

			fileMap := make(map[string]bool)
			for _, f := range files {
				fileMap[f] = true
			}

			for name := range expectedFiles {
				require.True(t, fileMap[name], "Should include file: %s", name)
			}
		})
	}
}
