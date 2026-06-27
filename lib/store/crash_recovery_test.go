package store

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/memsize"
)

func TestCrashRecovery(t *testing.T) {
	t.Run("complete blobs, evictability, and completeness are recovered", func(t *testing.T) {
		require := require.New(t)
		store, rootDir := newTestStore(t, 10*memsize.KB)

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
		_ = fillWithRandomData(t, incompleteEvictableF, 2*memsize.KB)
		require.NoError(incompleteEvictableF.Close())

		incompleteUnevictableF, incompleteUnevictableKey := newTestFile(t, store, 2*memsize.KB)
		_ = fillWithRandomData(t, incompleteUnevictableF, 2*memsize.KB)
		require.NoError(incompleteUnevictableF.Close())
		require.NoError(store.BanEviction(incompleteUnevictableKey))

		// Assume that the application crashes here. The application would restart and call `NewDiskStore`.
		rebootedStore, err := NewDiskStore(10*memsize.KB, rootDir)
		require.NoError(err)

		// Incomplete files get dropped.
		_, err = rebootedStore.Stat(incompleteEvictableKey, CheckIncompleteBlobs)
		require.ErrorIs(err, os.ErrNotExist)
		_, err = rebootedStore.Stat(incompleteUnevictableKey, CheckIncompleteBlobs)
		require.ErrorIs(err, os.ErrNotExist)

		// Complete files are recovered.
		f, err := rebootedStore.Open(completeEvictableKey, IgnoreIncompleteBlobs)
		require.NoError(err)
		defer func(f io.Closer) { require.NoError(f.Close()) }(f)
		data, err := io.ReadAll(f)
		require.NoError(err)
		require.Equal(completeEvictableData, data)
		complete := true
		unevictable, err := rebootedStore.checkDiskIfUnevictable(completeEvictableKey, complete)
		require.NoError(err)
		require.False(unevictable)

		f, err = rebootedStore.Open(completeUnevictableKey, IgnoreIncompleteBlobs)
		require.NoError(err)
		defer func(f io.Closer) { require.NoError(f.Close()) }(f)
		data, err = io.ReadAll(f)
		require.NoError(err)
		require.Equal(completeUnevictableData, data)
		unevictable, err = rebootedStore.checkDiskIfUnevictable(completeUnevictableKey, complete)
		require.NoError(err)
		require.True(unevictable)
	})

	t.Run("metadata is recovered", func(t *testing.T) {
		require := require.New(t)
		store, rootDir := newTestStore(t, 10*memsize.KB)

		f, key := newTestFile(t, store, 2*memsize.KB)
		require.NoError(f.Close())
		require.NoError(store.MarkComplete(key))
		writtenMd := metadata.NewTorrentMeta(core.MetaInfoFixture())
		require.NoError(store.SetMetadata(key, writtenMd))

		// Assume that the application crashes here. The application would restart and call `NewDiskStore`.
		rebootedStore, err := NewDiskStore(10*memsize.KB, rootDir)
		require.NoError(err)

		var readMd metadata.TorrentMeta
		ok, err := rebootedStore.GetMetadata(key, &readMd, IgnoreIncompleteBlobs)
		require.NoError(err)
		require.True(ok)
		require.Equal(writtenMd.MetaInfo, readMd.MetaInfo)
	})

	t.Run("lru order is approximated and blobs are evicted if store size exceeds capacity", func(t *testing.T) {
		require := require.New(t)
		store, rootDir := newTestStore(t, 10*memsize.KB)

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
		require.Equal([]string{cKey, dKey, eKey}, store.evictionOrder()) // a is unevictable and b is incomplete

		// reset the access time for d
		dF, err := store.Open(dKey, IgnoreIncompleteBlobs)
		require.NoError(err)
		require.NoError(dF.Close())
		evictionOrderBeforeCrash := store.evictionOrder()
		require.Equal([]string{cKey, eKey, dKey}, evictionOrderBeforeCrash) // a is unevictable and b is incomplete

		// Assume that the application restarts here.
		rebootedStore, err := NewDiskStore(10*memsize.KB, rootDir)
		require.NoError(err)

		// LRU order is approximated, but not exact.
		rebootedEvictionOrder := rebootedStore.evictionOrder()
		require.NotEqual(evictionOrderBeforeCrash, rebootedEvictionOrder)
		wantEvictionOrder := []string{cKey, dKey, eKey}
		require.Equal(wantEvictionOrder, rebootedEvictionOrder)

		// Assume we redeploy the service with a smaller capacity for the disk store:
		rebootedSmallerStore, err := NewDiskStore(6*memsize.KB, rootDir)
		require.NoError(err)

		// since 10KB of blobs are in store, `c` gets evicted to put the store back within its capacity.
		_, err = rebootedSmallerStore.Stat(cKey, CheckIncompleteBlobs)
		require.ErrorIs(err, os.ErrNotExist)

		require.Equal([]string{dKey, eKey}, rebootedSmallerStore.evictionOrder())
		_, err = rebootedSmallerStore.Stat(aKey, CheckIncompleteBlobs)
		require.NoError(err)
	})
}

func TestStoreWorksWhenFileSizeNotCorrect(t *testing.T) {
	// Verify that the store works correctly when the reserved size for a file (the one passed by the client in Create) is different
	// than its actual size. The store is expected to consistently use EITHER the client-given size OR the actual size of files, but not both.
	// If we mix them, this could break the eviction logic - imagine the user uploads a 2GB size but reports it as 1.9GB. Eviction works correctly
	// as long as we reserve 2GB upon upload to store and release 2GB upon deletion/eviction from store. BUT if we reserve 2GB and free 1.9GB
	// or vice-versa, it could lead to over/under-reservation.
	t.Skip("TODO")
}

func fillWithRandomData(t *testing.T, f FileReadWriter, sizeBytes uint64) []byte {
	data := make([]byte, sizeBytes)
	_, err := rand.Read(data)
	require.NoError(t, err)
	_, err = io.Copy(f, bytes.NewReader(data))
	require.NoError(t, err)
	return data
}
