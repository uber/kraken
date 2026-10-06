package tiered

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/memsize"
	"github.com/uber/kraken/utils/testutil"
)

func TestFile_MemToDiskLifecycle(t *testing.T) {
	require := require.New(t)
	// Mem capacity fits exactly one blob at a time, forcing eviction once a
	// second blob is created after the first is flushed and unbanned.
	s, _ := newTestStore(t, 10*memsize.KB, 1*memsize.KB, 1)

	key, data := createAndWrite(t, s, 1*memsize.KB)
	require.NoError(s.MarkComplete(key))
	waitForFlushed(t, s, key)

	f, err := s.Open(key)
	require.NoError(err)
	defer func() { require.NoError(f.Close()) }()

	// Creating a same-sized filler blob synchronously evicts key from mem,
	// since it's the only evictable, complete blob and capacity is exactly 1KB.
	_, _ = createAndWrite(t, s, 1*memsize.KB)
	_, inMem := s.impl.mem.Has(key)
	require.False(inMem, "key should have been evicted from mem to make room for the filler blob")

	// The already-open File must transparently continue to work, now served from disk.
	got, err := io.ReadAll(f)
	require.NoError(err)
	require.Equal(data, got)
}

func TestFile_ParallelAccessDuringMemToDiskTransition(t *testing.T) {
	require := require.New(t)
	s, _ := newTestStore(t, 10*memsize.KB, 1*memsize.KB, 1)

	const chunkSize = 10
	const numChunks = 5
	data := randomData(t, chunkSize*numChunks)
	key := core.DigestFixture().Hex()
	f, err := s.Create(key, uint64(len(data)))
	require.NoError(err)
	_, err = f.Write(data)
	require.NoError(err)
	require.NoError(f.Close())
	require.NoError(s.MarkComplete(key))
	waitForFlushed(t, s, key)

	readF, err := s.Open(key)
	require.NoError(err)
	defer func() { require.NoError(readF.Close()) }()

	const numReadsPerGoroutine = 50
	var wg sync.WaitGroup
	wg.Add(numChunks)
	errs := make([]error, numChunks)
	for i := range numChunks {
		go func(i int) {
			defer wg.Done()
			want := data[i*chunkSize : (i+1)*chunkSize]
			buf := make([]byte, chunkSize)
			for range numReadsPerGoroutine {
				n, err := readF.ReadAt(buf, int64(i*chunkSize))
				if err != nil {
					errs[i] = err
					return
				}
				if n != chunkSize || !bytes.Equal(buf, want) {
					errs[i] = fmt.Errorf("unexpected data on chunk %d: got %v, want %v", i, buf, want)
					return
				}
			}
		}(i)
	}

	// Force a real mem eviction of key while the reads above are in flight.
	_, _ = createAndWrite(t, s, 1*memsize.KB)

	wg.Wait()
	for i := range numChunks {
		require.NoError(errs[i])
	}

	_, inMem := s.impl.mem.Has(key)
	require.False(inMem)
}

// TestFile_BlobEvictedFromDiskBeforeMem exercises the edge case documented in
// file.go's openDiskFileIfNeeded TODO: a blob evicted from disk while still
// resident in mem. This requires very specific LRU timing and is unexpected
// in production - it's exercised here for documentation purposes.
func TestFile_BlobEvictedFromDiskBeforeMem(t *testing.T) {
	require := require.New(t)
	// Disk capacity fits only one blob; mem capacity fits key + otherKey.
	s, _ := newTestStore(t, 1*memsize.KB, 2*memsize.KB, 1)

	key, _ := createAndWrite(t, s, 1*memsize.KB)
	require.NoError(s.MarkComplete(key))
	waitForFlushed(t, s, key)

	// Open a handle to key while it's still resident in mem, before disk evicts it.
	f, err := s.Open(key)
	require.NoError(err)
	// f.Close() is a no-op on the mem side regardless of the invariant
	// violation triggered below, so it's still safe to assert on here.
	defer func() { require.NoError(f.Close()) }()

	// Completing otherKey forces disk (1KB capacity) to evict key to make room.
	otherKey, _ := createAndWrite(t, s, 1*memsize.KB)
	require.NoError(s.MarkComplete(otherKey))
	err = testutil.PollUntilTrue(5*time.Second, func() bool {
		_, inDisk := s.impl.disk.Has(key)
		return !inDisk
	})
	require.NoError(err)

	// Now evict key from mem too (capacity 2KB fits key + otherKey exactly;
	// one more filler forces out key, since it's LRU-oldest).
	_, _ = createAndWrite(t, s, 1*memsize.KB)
	_, inMem := s.impl.mem.Has(key)
	require.False(inMem)

	// The already-open handle can no longer switch over to disk, since disk evicted key first.
	buf := make([]byte, 10)
	_, err = f.Read(buf)
	require.Error(err)
	require.ErrorContains(err, "blob not found in neither memory store nor disk store")
}
