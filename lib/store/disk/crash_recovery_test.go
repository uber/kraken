package disk

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/memsize"
)

func TestCrashRecovery(t *testing.T) {
	t.Run("blobs, evictability, completeness, and size are recovered regardless of sharding", func(t *testing.T) {
		for _, shardLength := range []int{0, _defaultShardLength, 4} {
			require := require.New(t)

			rootDir, err := os.MkdirTemp("/tmp", "kraken-disk-store")
			require.NoError(err)
			t.Cleanup(func() { require.NoError(os.RemoveAll(rootDir)) })
			config := &Config{
				Capacity:              10 * memsize.KB,
				RootDir:               rootDir,
				RebootIncompleteBlobs: true,
				ShardLength:           shardLength,
			}

			store, err := NewStore(config, tally.NoopScope)
			require.NoError(err)

			completeEvictableF, completeEvictableKey := newTestFile(t, store, 2*memsize.KB)
			completeEvictableData := fillWithRandomData(t, completeEvictableF, 2*memsize.KB)
			// We don't have to test case where file is not closed, as linux closes all FDs owned by a process upon process death.
			require.NoError(completeEvictableF.Close())
			require.NoError(store.MarkComplete(completeEvictableKey))

			completeUnevictableF, completeUnevictableKey := newTestFile(t, store, 2*memsize.KB)
			completeUnevictableData := fillWithRandomData(t, completeUnevictableF, 2*memsize.KB)
			require.NoError(completeUnevictableF.Close())
			require.NoError(store.MarkComplete(completeUnevictableKey))
			require.NoError(store.BanEviction(completeUnevictableKey))

			incompleteEvictableF, incompleteEvictableKey := newTestFile(t, store, 2*memsize.KB)
			incompleteEvictableData := fillWithRandomData(t, incompleteEvictableF, 2*memsize.KB)
			require.NoError(incompleteEvictableF.Close())

			incompleteUnevictableF, incompleteUnevictableKey := newTestFile(t, store, 2*memsize.KB)
			incompleteUnevictableData := fillWithRandomData(t, incompleteUnevictableF, 2*memsize.KB)
			require.NoError(incompleteUnevictableF.Close())
			require.NoError(store.BanEviction(incompleteUnevictableKey))

			// Assume that the application crashes here. The application would restart and call `NewStore`.
			store, err = NewStore(&Config{10 * memsize.KB, rootDir, true, shardLength}, tally.NoopScope)
			require.NoError(err)

			require.Equal(8*memsize.KB, store.impl.size)

			// Incomplete files are recovered (since `rebootIncompleteBlobs` is true).
			f, err := store.Open(incompleteEvictableKey)
			require.NoError(err)
			defer func(f io.Closer) { require.NoError(f.Close()) }(f)
			data, err := io.ReadAll(f)
			require.NoError(err)
			require.Equal(incompleteEvictableData, data)
			unevictable, err := store.impl.checkDiskIfUnevictable(incompleteEvictableKey, _incompleteBlob)
			require.NoError(err)
			require.False(unevictable)

			f, err = store.Open(incompleteUnevictableKey)
			require.NoError(err)
			defer func(f io.Closer) { require.NoError(f.Close()) }(f)
			data, err = io.ReadAll(f)
			require.NoError(err)
			require.Equal(incompleteUnevictableData, data)
			unevictable, err = store.impl.checkDiskIfUnevictable(incompleteUnevictableKey, _incompleteBlob)
			require.NoError(err)
			require.True(unevictable)

			// Complete files are always recovered.
			f, err = store.ScopeComplete().Open(completeEvictableKey)
			require.NoError(err)
			defer func(f io.Closer) { require.NoError(f.Close()) }(f)
			data, err = io.ReadAll(f)
			require.NoError(err)
			require.Equal(completeEvictableData, data)
			unevictable, err = store.impl.checkDiskIfUnevictable(completeEvictableKey, _completeBlob)
			require.NoError(err)
			require.False(unevictable)

			f, err = store.ScopeComplete().Open(completeUnevictableKey)
			require.NoError(err)
			defer func(f io.Closer) { require.NoError(f.Close()) }(f)
			data, err = io.ReadAll(f)
			require.NoError(err)
			require.Equal(completeUnevictableData, data)
			unevictable, err = store.impl.checkDiskIfUnevictable(completeUnevictableKey, _completeBlob)
			require.NoError(err)
			require.True(unevictable)

			// The sizes of both incomplete and complete files are recovered correctly.
			require.Equal(8*memsize.KB, store.impl.size)

			// Run the store with `rebootIncompleteBlobs` as false.
			store, err = NewStore(&Config{10 * memsize.KB, rootDir, false, shardLength}, tally.NoopScope)
			require.NoError(err)

			// Incomplete files are dropped.
			rebootedKeys := store.List()
			require.NotContains(rebootedKeys, incompleteEvictableKey)
			require.NotContains(rebootedKeys, incompleteUnevictableKey)

			// Complete files are always recovered.
			f, err = store.ScopeComplete().Open(completeEvictableKey)
			require.NoError(err)
			defer func(f io.Closer) { require.NoError(f.Close()) }(f)
			data, err = io.ReadAll(f)
			require.NoError(err)
			require.Equal(completeEvictableData, data)
			unevictable, err = store.impl.checkDiskIfUnevictable(completeEvictableKey, _completeBlob)
			require.NoError(err)
			require.False(unevictable)

			f, err = store.ScopeComplete().Open(completeUnevictableKey)
			require.NoError(err)
			defer func(f io.Closer) { require.NoError(f.Close()) }(f)
			data, err = io.ReadAll(f)
			require.NoError(err)
			require.Equal(completeUnevictableData, data)
			unevictable, err = store.impl.checkDiskIfUnevictable(completeUnevictableKey, _completeBlob)
			require.NoError(err)
			require.True(unevictable)

			require.Equal(4*memsize.KB, store.impl.size)
		}
	})

	t.Run("metadata is recovered for complete blob", func(t *testing.T) {
		require := require.New(t)
		store, rootDir := newTestStore(t, 10*memsize.KB, true)

		f, key := newTestFile(t, store, 2*memsize.KB)
		require.NoError(f.Close())
		require.NoError(store.MarkComplete(key))
		writtenMd := metadata.NewTorrentMeta(core.MetaInfoFixture())
		require.NoError(store.SetMetadata(key, writtenMd))

		// Assume that the application crashes here. The application would restart and call `NewStore`.
		store, err := NewStore(&Config{10 * memsize.KB, rootDir, true, _defaultShardLength}, tally.NoopScope)
		require.NoError(err)

		var readMd metadata.TorrentMeta
		ok, err := store.ScopeComplete().GetMetadata(key, &readMd)
		require.NoError(err)
		require.True(ok)
		require.Equal(writtenMd.MetaInfo, readMd.MetaInfo)
	})

	t.Run("metadata is recovered for incomplete blob", func(t *testing.T) {
		require := require.New(t)
		store, rootDir := newTestStore(t, 10*memsize.KB, true)

		f, key := newTestFile(t, store, 2*memsize.KB)
		require.NoError(f.Close())
		writtenMd := metadata.NewTorrentMeta(core.MetaInfoFixture())
		require.NoError(store.SetMetadata(key, writtenMd))

		// Assume that the application crashes here. The application would restart and call `NewStore`.
		store, err := NewStore(&Config{10 * memsize.KB, rootDir, true, _defaultShardLength}, tally.NoopScope)
		require.NoError(err)

		var readMd metadata.TorrentMeta
		ok, err := store.GetMetadata(key, &readMd)
		require.NoError(err)
		require.True(ok)
		require.Equal(writtenMd.MetaInfo, readMd.MetaInfo)
	})

	t.Run("lru order is approximated and blobs are evicted if store size exceeds capacity", func(t *testing.T) {
		require := require.New(t)
		store, rootDir := newTestStore(t, 10*memsize.KB, false)

		aF, aKey := newTestFile(t, store, 2*memsize.KB)
		_ = fillWithRandomData(t, aF, 2*memsize.KB)
		require.NoError(aF.Close())
		require.NoError(store.MarkComplete(aKey))
		require.NoError(store.BanEviction(aKey))

		bF, _ := newTestFile(t, store, 2*memsize.KB)
		_ = fillWithRandomData(t, bF, 1*memsize.KB)
		require.NoError(bF.Close())

		cF, cKey := newTestFile(t, store, 2*memsize.KB)
		_ = fillWithRandomData(t, cF, 2*memsize.KB)
		require.NoError(cF.Close())
		require.NoError(store.MarkComplete(cKey))

		dF, dKey := newTestFile(t, store, 2*memsize.KB)
		_ = fillWithRandomData(t, dF, 2*memsize.KB)
		require.NoError(dF.Close())
		require.NoError(store.MarkComplete(dKey))

		eF, eKey := newTestFile(t, store, 2*memsize.KB)
		_ = fillWithRandomData(t, eF, 2*memsize.KB)
		require.NoError(eF.Close())
		require.NoError(store.MarkComplete(eKey))
		require.Equal([]string{cKey, dKey, eKey}, store.impl.evictionOrder()) // a is unevictable and b is incomplete

		// reset the access time for d
		dF, err := store.ScopeComplete().Open(dKey)
		require.NoError(err)
		require.NoError(dF.Close())
		evictionOrderBeforeCrash := store.impl.evictionOrder()
		require.Equal([]string{cKey, eKey, dKey}, evictionOrderBeforeCrash) // a is unevictable and b is incomplete

		// Assume that the application restarts here.
		store, err = NewStore(&Config{10 * memsize.KB, rootDir, false, _defaultShardLength}, tally.NoopScope)
		require.NoError(err)

		// LRU order is approximated, but not exact.
		rebootedEvictionOrder := store.impl.evictionOrder()
		require.NotEqual(evictionOrderBeforeCrash, rebootedEvictionOrder)
		wantEvictionOrder := []string{cKey, dKey, eKey}
		require.Equal(wantEvictionOrder, rebootedEvictionOrder)

		// Assume we redeploy the service with a smaller capacity for the disk store:
		store, err = NewStore(&Config{6 * memsize.KB, rootDir, false, _defaultShardLength}, tally.NoopScope)
		require.NoError(err)

		// since 10KB of blobs are in store, `c` gets evicted to put the store back within its capacity.
		require.NotContains(store.List(), cKey)

		require.Equal([]string{dKey, eKey}, store.impl.evictionOrder())
		require.Contains(store.List(), aKey)
	})

	t.Run("reboot fails if unevictable data is more than store capacity", func(t *testing.T) {
		require := require.New(t)
		store, rootDir := newTestStore(t, 10*memsize.KB, true)

		aF, aKey := newTestFile(t, store, 2*memsize.KB)
		_ = fillWithRandomData(t, aF, 2*memsize.KB)
		require.NoError(aF.Close())
		require.NoError(store.MarkComplete(aKey))
		require.NoError(store.BanEviction(aKey))

		bF, _ := newTestFile(t, store, 2*memsize.KB)
		_ = fillWithRandomData(t, bF, 1*memsize.KB)
		require.NoError(bF.Close())

		cF, _ := newTestFile(t, store, 2*memsize.KB)
		_ = fillWithRandomData(t, cF, 1*memsize.KB)
		require.NoError(cF.Close())

		// Assume that the application restarts here.
		_, err := NewStore(&Config{5 * memsize.KB, rootDir, true, _defaultShardLength}, tally.NoopScope)
		require.ErrorIs(err, errNoSpace)
	})
}

func TestIncompleteBlobDownloadResumedAfterMultipleCrashes(t *testing.T) {
	require := require.New(t)
	store, rootDir := newTestStore(t, 10*memsize.KB, true)

	f, key := newTestFile(t, store, 4*memsize.KB)
	firstData := fillWithRandomData(t, f, 2*memsize.KB)

	// First crash.
	store, err := NewStore(&Config{10 * memsize.KB, rootDir, true, _defaultShardLength}, tally.NoopScope)
	require.NoError(err)

	f, err = store.Open(key)
	require.NoError(err)

	// Write 5KB in total, while reporting only 4KB.
	secondData := make([]byte, 3*memsize.KB)
	_, err = rand.Read(secondData)
	require.NoError(err)
	_, err = f.WriteAt(secondData, int64(2*memsize.KB))
	require.NoError(err)

	// Second crash.
	store, err = NewStore(&Config{10 * memsize.KB, rootDir, true, _defaultShardLength}, tally.NoopScope)
	require.NoError(err)

	wantData := make([]byte, 5*memsize.KB)
	copy(wantData, firstData)
	copy(wantData[2*memsize.KB:], secondData)

	f, err = store.Open(key)
	require.NoError(err)
	data, err := io.ReadAll(f)
	require.NoError(err)
	require.Equal(wantData, data)
	// The declared size is recovered correctly both times, without leaking or duplicating reservation.
	require.Equal(4*memsize.KB, store.impl.size)
	require.NoError(store.MarkComplete(key))

	// Third crash.
	store, err = NewStore(&Config{10 * memsize.KB, rootDir, true, _defaultShardLength}, tally.NoopScope)
	require.NoError(err)
	// The blob's client-provided size should be rebooted through from the _size sidecar file.
	require.Equal(4*memsize.KB, store.impl.size)
	f, err = store.ScopeComplete().Open(key)
	require.NoError(err)
	defer func(f io.Closer) { require.NoError(f.Close()) }(f)
	data, err = io.ReadAll(f)
	require.NoError(err)
	require.Equal(wantData, data)
}

func TestStoreWorksWhenFileSizeNotCorrect(t *testing.T) {
	// Verify that the store works correctly when the reserved size for a file (the one passed by the client in Create) is different
	// than its actual size. The store is expected to consistently use only the client-given size and never the blob'sreal size.
	// If we mix them, this could break the eviction logic - imagine the user uploads a 2GB size but reports it as 1.9GB. Eviction works correctly
	// as long as we reserve 2GB upon upload to store and release 2GB upon deletion/eviction from store. BUT if we reserve 2GB and free 1.9GB
	// or vice-versa, it could lead to over/under-reservation.
	require := require.New(t)
	store, _ := newTestStore(t, 10*memsize.KB, false)

	// Declares 8KB but only writes 1KB.
	underreportedF, underreportedKey := newTestFile(t, store, 8*memsize.KB)
	_ = fillWithRandomData(t, underreportedF, 1*memsize.KB)
	require.NoError(underreportedF.Close())
	require.NoError(store.MarkComplete(underreportedKey))
	require.NoError(store.BanEviction(underreportedKey))
	require.Equal(8*memsize.KB, store.impl.size)

	// Simulate crash and restart.
	store, err := NewStore(store.impl.config, tally.NoopScope)
	require.NoError(err)

	// Even after the crash, the client-provided blob size is used and not the blob's real size.
	require.Equal(8*memsize.KB, store.impl.size)

	// Even though only 1KB is actually used on disk, the store enforces capacity based on the
	// declared 8KB, so a 3KB blob doesn't fit alongside it (there's nothing evictable to make room).
	_, err = store.Create(core.DigestFixture().Hex(), 3*memsize.KB)
	require.ErrorIs(err, errNoSpace)

	// Declares 2KB (fits exactly within the remaining capacity) but writes 5KB.
	overreportedF, overreportedKey := newTestFile(t, store, 2*memsize.KB)
	_ = fillWithRandomData(t, overreportedF, 5*memsize.KB)
	require.NoError(overreportedF.Close())
	require.NoError(store.MarkComplete(overreportedKey))
	// The store still only accounts for the declared 2KB, not the actual 5KB written.
	require.Equal(10*memsize.KB, store.impl.size)

	// Deleting releases exactly the declared size that was reserved, not the actual bytes found on
	// disk, keeping reservation accounting self-consistent in both directions.
	require.NoError(store.Delete(overreportedKey))
	require.Equal(8*memsize.KB, store.impl.size)
	require.NoError(store.Delete(underreportedKey))
	require.Equal(uint64(0), store.impl.size)
}

func fillWithRandomData(t *testing.T, f storelib.FileReadWriter, sizeBytes uint64) []byte {
	data := make([]byte, sizeBytes)
	_, err := rand.Read(data)
	require.NoError(t, err)
	_, err = io.Copy(f, bytes.NewReader(data))
	require.NoError(t, err)
	return data
}
