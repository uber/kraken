package store

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/memsize"
)

const (
	// TODO - consider exporting these constants for clients to consume to avoid naked params
	_dontIgnoreIncompleteFiles = false
	_ignoreIncompleteFiles     = true
)

func newTestStore(t *testing.T, capacity uint64) (res *DiskStore, rootDir string) {
	rootDir, err := os.MkdirTemp("/tmp", "kraken-disk-store")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(rootDir) })

	store, err := NewDiskStore(capacity, rootDir)
	require.NoError(t, err)
	return store, rootDir
}

func newTestFile(t *testing.T, store *DiskStore, size uint64) (f FileReadWriter, key string) {
	require := require.New(t)
	key = core.DigestFixture().Hex()
	var err error
	f, err = store.Create(key, size)
	require.NoError(err)
	return f, key
}

// does not count 1) the directories for sharding, 2) metadata files, and 3) the _eviction_banned flag file.
func numBlobsOnDisk(t *testing.T, store *DiskStore) int {
	numBlobs := 0
	err := filepath.WalkDir(store.dir, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !strings.HasSuffix(path, _blobFileName) {
			return nil
		}

		numBlobs++
		return nil
	})
	require.NoError(t, err)
	return numBlobs
}

func TestDiskStore(t *testing.T) {
	require := require.New(t)
	store, _ := newTestStore(t, 10*memsize.KB)

	digests := []core.Digest{}
	for i := range 10 {
		digest := core.DigestFixture()
		digests = append(digests, digest)
		f, err := store.Create(digest.Hex(), memsize.KB)
		require.NoError(err)

		data := make([]byte, memsize.KB)
		for k := range data {
			data[k] = byte(i + 1)
		}
		n, err := io.Copy(f, bytes.NewReader(make([]byte, memsize.KB)))
		require.Equal(n, int64(memsize.KB))
		require.NoError(err)
	}
	require.Equal(store.size, 10*memsize.KB)

	digest := core.DigestFixture()
	writer, err := store.Create(digest.Hex(), memsize.B)
	require.EqualError(err, "reserve space: cannot evict enough, the unevictable/incomplete blobs are using up all the space")
	require.Nil(writer)

	reader, err := store.Open(digests[0].Hex(), true)
	require.Equal(err, os.ErrNotExist)
	require.Nil(reader)

	require.NoError(store.MarkComplete(digests[0].Hex()))
	reader, err = store.Open(digests[0].Hex(), true)
	require.NoError(err)
	wantData := make([]byte, memsize.KB)
	for k := range wantData {
		wantData[k] = byte(1)
	}
	require.NoError(iotest.TestReader(reader, make([]byte, memsize.KB)))

	// now test that LRU logic works - make sure that the 1 that is complete gets evicted.
	digest = core.DigestFixture()
	writer, err = store.Create(digest.Hex(), memsize.KB)
	require.NoError(err)
	reader, err = store.Open(digests[0].Hex(), true)
	require.Equal(err, os.ErrNotExist)
	require.Nil(reader)
}

func TestEviction(t *testing.T) {
	require := require.New(t)
	store, _ := newTestStore(t, 25*memsize.KB)
	// create 5 blobs - a, b, c, d, e with different sizes.
	a, aKey := newTestFile(t, store, 10*memsize.KB)
	require.NoError(a.Close())
	b, bKey := newTestFile(t, store, 5*memsize.KB)
	require.NoError(b.Close())
	c, cKey := newTestFile(t, store, 5*memsize.KB)
	require.NoError(c.Close())
	d, dKey := newTestFile(t, store, 3*memsize.KB)
	require.NoError(d.Close())
	e, eKey := newTestFile(t, store, 1*memsize.KB)
	require.NoError(e.Close())

	require.Equal(24*memsize.KB, store.size)
	require.Equal(5, numBlobsOnDisk(t, store))
	// incomplete files cannot be evicted and adding 2KB would result in overreservation.
	_, err := store.Create(core.DigestFixture().Hex(), 2*memsize.KB)
	require.EqualError(err, "reserve space: cannot evict enough, the unevictable/incomplete blobs are using up all the space")
	// start marking as complete in this specific order - c, b, a (MarkComplete resets access time).
	require.NoError(store.MarkComplete(cKey))
	require.NoError(store.MarkComplete(bKey))
	require.NoError(store.MarkComplete(aKey))
	// d is complete but its eviction is banned.
	require.NoError(store.MarkComplete(dKey))
	require.NoError(store.BanEviction(dKey))
	// e is banned from eviction before it even becomes complete.
	require.NoError(store.BanEviction(eKey))
	require.Equal(24*memsize.KB, store.size)
	// Add f (4KB) which should evict c to make space, as d and e are unevictable and c was accessed last (the MarkComplete call).
	f, fKey := newTestFile(t, store, 4*memsize.KB)
	require.NoError(f.Close())
	require.NoError(store.MarkComplete(fKey))
	keys := store.List(_dontIgnoreIncompleteFiles)
	require.NotContains(keys, cKey)
	// new size == 23KB == 24KB - 5KB (c) + 4KB (f)
	require.Equal(23*memsize.KB, store.size)
	require.Equal(5, numBlobsOnDisk(t, store))

	// Add g (1KB), which will not evict anything
	g, gKey := newTestFile(t, store, 1*memsize.KB)
	require.NoError(g.Close())
	require.NoError(store.MarkComplete(gKey))
	require.Equal(24*memsize.KB, store.size)
	require.Equal(6, numBlobsOnDisk(t, store))

	// Add h (15KB), which evicts b and a:
	h, hKey := newTestFile(t, store, 15*memsize.KB)
	require.NoError(h.Close())
	require.NoError(store.MarkComplete(hKey))
	// size == 24KB == 24KB + 15KB (h) - 5KB (b) - 10KB (a)
	require.Equal(24*memsize.KB, store.size)
	require.Equal(5, numBlobsOnDisk(t, store))
	keys = store.List(_dontIgnoreIncompleteFiles)
	require.NotContains(keys, bKey)
	require.NotContains(keys, aKey)

	// allow e to be evicted.
	require.NoError(store.MarkComplete(eKey))
	require.NoError(store.UnbanEviction(eKey))
	// eviction order (left-most is next to evict): f(4KB), g(1KB), h(15KB), e(1KB); d(3KB) is unevictable
	// we open g to change the order to f, h, e, g
	g, err = store.Open(gKey, _dontIgnoreIncompleteFiles)
	require.NoError(err)
	require.NoError(g.Close())

	i, iKey := newTestFile(t, store, 5*memsize.KB)
	require.NoError(store.MarkComplete(iKey))
	require.NoError(i.Close())
	keys = store.List(_dontIgnoreIncompleteFiles)
	require.NotContains(keys, fKey)
	require.Equal(25*memsize.KB, store.size)
	require.Equal(5, numBlobsOnDisk(t, store))
	// eviction order: h(15KB), e(1KB), g(1KB), i(5KB); d(3KB) is unevictable

	j, jKey := newTestFile(t, store, 14*memsize.KB)
	require.NoError(j.Close())
	require.NoError(store.MarkComplete(jKey))
	keys = store.List(_dontIgnoreIncompleteFiles)
	require.NotContains(keys, hKey)
	require.Equal(24*memsize.KB, store.size)
	require.Equal(5, numBlobsOnDisk(t, store))
	// eviction order: e(1KB), g(1KB), i(5KB), j(14KB); d(3KB) is unevictable

	k, kKey := newTestFile(t, store, 2*memsize.KB)
	require.NoError(k.Close())
	require.NoError(store.MarkComplete(kKey))
	keys = store.List(_dontIgnoreIncompleteFiles)
	require.NotContains(keys, eKey)
	require.Equal(25*memsize.KB, store.size)
	require.Equal(5, numBlobsOnDisk(t, store))
	// eviction order: g(1KB), i(5KB), j(14KB), k(2KB); d(3KB) is unevictable

	l, lKey := newTestFile(t, store, 1*memsize.KB)
	require.NoError(store.MarkComplete(lKey))
	require.NoError(l.Close())
	keys = store.List(_dontIgnoreIncompleteFiles)
	require.NotContains(keys, gKey)
	require.Equal(25*memsize.KB, store.size)
	require.Equal(5, numBlobsOnDisk(t, store))
	// eviction order: i(5KB), j(14KB), k(2KB), l(1KB); d(3KB) is unevictable

	require.NoError(store.Delete(iKey))
	require.NoError(store.Delete(jKey))
	require.NoError(store.Delete(lKey))
	// evictionOrder: k(2KB); d(3KB)
	require.Equal(5*memsize.KB, store.size)
	require.Equal(2, numBlobsOnDisk(t, store))
}

func TestParallelAccessToSingleFile(t *testing.T) {
	require := require.New(t)
	store, _ := newTestStore(t, 10*memsize.KB)

	key := core.DigestFixture().Hex()
	f, err := store.Create(key, 1*memsize.KB)
	require.NoError(err)
	require.NoError(f.Close())
	// Spawn 5 routines in parallel that write and read to different parts of the file.
	var wg sync.WaitGroup
	wg.Add(5)

	for idx := range 5 {
		go func(idx int64) {
			defer wg.Done()

			ignoreIncomplete := false
			f, err := store.Open(key, ignoreIncomplete)
			require.NoError(err)
			pos := idx * 10
			writtenData := make([]byte, 10)
			for k := range writtenData {
				writtenData[k] = byte(idx)
			}
			n, err := f.WriteAt(writtenData, pos)
			require.NoError(err)
			require.Equal(10, n)

			defer func() { require.NoError(f.Close()) }()
			readData := make([]byte, 10)
			n, err = f.ReadAt(readData, pos)
			require.NoError(err)
			require.Equal(10, n)
			require.Equal(writtenData, readData)
		}(int64(idx))
	}

	wg.Wait()
	require.NoError(store.MarkComplete(key))

	ignoreIncomplete := true
	f, err = store.Open(key, ignoreIncomplete)
	require.NoError(err)
	defer func() { require.NoError(f.Close()) }()

	wantFileData := make([]byte, 50)
	for i := range 50 {
		wantFileData[i] = byte(i / 10)
	}
	fData, err := io.ReadAll(f)
	require.NoError(err)
	require.Equal(wantFileData, fData)
}

func TestOpenedFileAccessibleAfterMarkedComplete(t *testing.T) {
	require := require.New(t)
	store, _ := newTestStore(t, 10*memsize.KB)

	key := core.DigestFixture().Hex()
	f, err := store.Create(key, 1*memsize.KB)
	require.NoError(err)
	_, err = io.Copy(f, bytes.NewReader([]byte("Hello World")))
	require.NoError(err)
	require.NoError(f.Close())

	ignoreIncomplete := false
	incompleteFile, err := store.Open(key, ignoreIncomplete)
	require.NoError(err)
	defer func() { require.NoError(incompleteFile.Close()) }()

	require.NoError(store.MarkComplete(key))

	ignoreIncomplete = true
	completeFile, err := store.Open(key, ignoreIncomplete)
	require.NoError(err)
	defer func() { require.NoError(completeFile.Close()) }()

	incompleteFileData, err := io.ReadAll(incompleteFile)
	require.NoError(err)
	completeFileData, err := io.ReadAll(completeFile)
	require.NoError(err)

	require.Equal([]byte("Hello World"), incompleteFileData)
	require.Equal([]byte("Hello World"), completeFileData)
}

func TestOpenedFileAccessibleAfterEviction(t *testing.T) {
	// when a file is created, then received with a Get, then deleted, check that the store doesn't count its size but it is still on disk and accessible. then check that when the client closes the file, it is not accessible anymore and not on disk anymore.
	// test after both "Delete" and natural LRU eviction.
	// what happens when we try to close the fd? i assume it works?
	t.Skip("TODO")
}

func TestDelete(t *testing.T) {
	t.Run("incomplete blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(f.Close())
		require.NoError(store.Delete(key))

		require.Empty(store.List(false))
		require.Equal(uint64(0), store.size)
	})
	t.Run("incomplete, unevictable blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(f.Close())
		require.NoError(store.BanEviction(key))

		require.NoError(store.Delete(key))

		require.Empty(store.List(false))
		require.Equal(uint64(0), store.size)
	})
	t.Run("complete blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(f.Close())
		require.NoError(store.MarkComplete(key))

		require.NoError(store.Delete(key))

		require.Empty(store.List(false))
		require.Equal(uint64(0), store.size)
	})
	t.Run("complete, unevictable blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(f.Close())
		require.NoError(store.MarkComplete(key))
		require.NoError(store.BanEviction(key))

		require.NoError(store.Delete(key))

		require.Empty(store.List(false))
		require.Equal(uint64(0), store.size)
	})
	t.Run("not found", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()

		err := store.Delete(key)
		require.Equal(os.ErrNotExist, err)
	})
}

func TestMarkComplete(t *testing.T) {
	t.Run("incomplete blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)

		require.NoError(store.MarkComplete(key))

		require.Equal([]string{key}, store.List(true))
		require.Equal(uint64(100), store.size)
		_, err = store.Open(key, true)
		require.NoError(err)
	})
	t.Run("incomplete blob with forbidden eviction", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(store.BanEviction(key))

		require.NoError(store.MarkComplete(key))

		require.Equal([]string{key}, store.List(true))
		require.Equal(uint64(100), store.size)
		_, err = store.Open(key, true)
		require.NoError(err)
	})
	t.Run("already complete blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(store.MarkComplete(key))

		require.NoError(store.MarkComplete(key))
	})
	t.Run("already complete blob with forbidden eviction", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(store.MarkComplete(key))
		require.NoError(store.BanEviction(key))

		require.NoError(store.MarkComplete(key))
	})
	t.Run("not found", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()

		err := store.MarkComplete(key)
		require.Equal(os.ErrNotExist, err)
	})
}

func TestStat(t *testing.T) {
	t.Run("complete blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 10*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 10)))
		require.NoError(err)
		require.NoError(store.MarkComplete(key))

		fInfo, err := store.Stat(key, true)
		require.NoError(err)
		_, err = store.Stat(key, false)
		require.NoError(err)

		require.False(fInfo.IsDir())
		require.WithinDuration(time.Now(), fInfo.ModTime(), 500*time.Millisecond)
		require.Equal(_blobFileName, fInfo.Name())
		require.Equal(int64(10), fInfo.Size())
	})
	t.Run("complete, unevictable blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 10*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 10)))
		require.NoError(err)
		require.NoError(store.MarkComplete(key))
		require.NoError(store.BanEviction(key))

		fInfo, err := store.Stat(key, true)
		require.NoError(err)
		_, err = store.Stat(key, false)
		require.NoError(err)

		require.False(fInfo.IsDir())
		require.WithinDuration(time.Now(), fInfo.ModTime(), 500*time.Millisecond)
		require.Equal(_blobFileName, fInfo.Name())
		require.Equal(int64(10), fInfo.Size())
	})
	t.Run("incomplete blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 10*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 10)))
		require.NoError(err)

		_, err = store.Stat(key, true)
		require.Equal(os.ErrNotExist, err)
		fInfo, err := store.Stat(key, false)
		require.NoError(err)

		require.False(fInfo.IsDir())
		require.WithinDuration(time.Now(), fInfo.ModTime(), 500*time.Millisecond)
		require.Equal(_blobFileName, fInfo.Name())
		require.Equal(int64(10), fInfo.Size())
	})

	t.Run("incomplete, unevictable blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 10*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 10)))
		require.NoError(err)
		require.NoError(store.BanEviction(key))

		_, err = store.Stat(key, true)
		require.Equal(os.ErrNotExist, err)
		fInfo, err := store.Stat(key, false)
		require.NoError(err)

		require.False(fInfo.IsDir())
		require.WithinDuration(time.Now(), fInfo.ModTime(), 500*time.Millisecond)
		require.Equal(_blobFileName, fInfo.Name())
		require.Equal(int64(10), fInfo.Size())
	})
	t.Run("non-existent blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()

		_, err := store.Stat(key, true)
		require.Equal(os.ErrNotExist, err)
		_, err = store.Stat(key, false)
		require.Equal(os.ErrNotExist, err)
	})
}

func TestList(t *testing.T) {
	require := require.New(t)
	store, _ := newTestStore(t, 10*memsize.KB)

	require.Empty(store.List(false))
	require.Empty(store.List(true))

	incompleteBlobKey := core.DigestFixture().Hex()
	f, err := store.Create(incompleteBlobKey, 10*memsize.B)
	require.NoError(err)
	defer func(f io.Closer) { require.NoError(f.Close()) }(f)
	_, err = io.Copy(f, bytes.NewReader(make([]byte, 10)))
	require.NoError(err)
	completeBlobKey := core.DigestFixture().Hex()
	f, err = store.Create(completeBlobKey, 10*memsize.B)
	require.NoError(err)
	_, err = io.Copy(f, bytes.NewReader(make([]byte, 10)))
	require.NoError(err)
	defer func(f io.Closer) { require.NoError(f.Close()) }(f)
	require.NoError(store.MarkComplete(completeBlobKey))
	unevictableIncompleteBlobKey := core.DigestFixture().Hex()
	f, err = store.Create(unevictableIncompleteBlobKey, 10*memsize.B)
	require.NoError(err)
	_, err = io.Copy(f, bytes.NewReader(make([]byte, 10)))
	require.NoError(err)
	defer func(f io.Closer) { require.NoError(f.Close()) }(f)
	unevictableCompleteBlobKey := core.DigestFixture().Hex()
	f, err = store.Create(unevictableCompleteBlobKey, 10*memsize.B)
	require.NoError(err)
	_, err = io.Copy(f, bytes.NewReader(make([]byte, 10)))
	require.NoError(err)
	defer func() { require.NoError(f.Close()) }()
	require.NoError(store.MarkComplete(unevictableCompleteBlobKey))

	require.Equal([]string{completeBlobKey, unevictableCompleteBlobKey}, store.List(true))
	require.Equal([]string{completeBlobKey, unevictableCompleteBlobKey, incompleteBlobKey, unevictableIncompleteBlobKey}, store.List(false))
}

func TestMetadata(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 10*memsize.KB)
		require.NoError(err)
		require.NoError(f.Close())

		mdStruct := core.MetaInfoFixture()
		writtenMd := metadata.NewTorrentMeta(mdStruct)
		err = store.SetMetadata(key, writtenMd)
		// ensure metadata is not included in LRU eviction calculation.
		require.NoError(err)

		var readMd metadata.TorrentMeta
		ok, err := store.GetMetadata(key, &readMd, _dontIgnoreIncompleteFiles)
		require.NoError(err)
		require.True(ok)
		require.Equal(readMd.MetaInfo, writtenMd.MetaInfo)

		require.NoError(store.DeleteMetadata(key, &readMd))
		ok, err = store.GetMetadata(key, &readMd, _dontIgnoreIncompleteFiles)
		require.NoError(err)
		require.False(ok)
		mdFilePath := store.sidecarFilePath(key, false, readMd.GetSuffix())
		// ensure the metadata file is deleted from disk
		_, err = os.Stat(mdFilePath)
		require.True(errors.Is(err, os.ErrNotExist))
		// deleting a second time should be a no-op.
		require.NoError(store.DeleteMetadata(key, &readMd))
	})

	t.Run("non-existant blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		nonExistentKey := core.DigestFixture().Hex()
		mdStruct := core.MetaInfoFixture()
		md := metadata.NewTorrentMeta(mdStruct)

		err := store.SetMetadata(nonExistentKey, md)
		require.Equal(os.ErrNotExist, err)

		ok, err := store.GetMetadata(nonExistentKey, md, _dontIgnoreIncompleteFiles)
		require.Equal(os.ErrNotExist, err)
		require.False(ok)

		err = store.DeleteMetadata(nonExistentKey, md)
		require.Equal(os.ErrNotExist, err)
	})

	t.Run("metadata does not change after marking a file as complete and/or evictable/unevictable", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 1*memsize.KB)
		require.NoError(err)
		require.NoError(f.Close())

		mdStruct := core.MetaInfoFixture()
		writtenMd := metadata.NewTorrentMeta(mdStruct)
		require.NoError(store.SetMetadata(key, writtenMd))

		var readMd metadata.TorrentMeta
		ok, err := store.GetMetadata(key, &readMd, _ignoreIncompleteFiles)
		require.Equal(os.ErrNotExist, err)
		// incomplete files are ignored
		require.False(ok)

		ok, err = store.GetMetadata(key, &readMd, _dontIgnoreIncompleteFiles)
		require.NoError(err)
		require.True(ok)
		require.Equal(readMd.MetaInfo, writtenMd.MetaInfo)

		// Repeat the tests above for an evictable file
		require.NoError(store.BanEviction(key))
		ok, err = store.GetMetadata(key, &readMd, _ignoreIncompleteFiles)
		require.Equal(os.ErrNotExist, err)
		// incomplete files are ignored
		require.False(ok)

		ok, err = store.GetMetadata(key, &readMd, _dontIgnoreIncompleteFiles)
		require.NoError(err)
		require.True(ok)
		require.Equal(readMd.MetaInfo, writtenMd.MetaInfo)

		require.NoError(store.MarkComplete(key))
		ok, err = store.GetMetadata(key, &readMd, _ignoreIncompleteFiles)
		require.NoError(err)
		require.True(ok)
		require.Equal(readMd.MetaInfo, writtenMd.MetaInfo)
	})
	t.Run("metadata fully gone after blob is evicted", func(t *testing.T) {
		require := require.New(t)
		store, _ := newTestStore(t, 10*memsize.KB)
		keyA := core.DigestFixture().Hex()
		fA, err := store.Create(keyA, 10*memsize.KB)
		require.NoError(err)
		require.NoError(fA.Close())
		require.NoError(store.MarkComplete(keyA))

		md := metadata.NewTorrentMeta(core.MetaInfoFixture())
		err = store.SetMetadata(keyA, md)
		store.SetMetadata(keyA, md)
		complete := true
		mdFilePath := store.sidecarFilePath(keyA, complete, md.GetSuffix())
		_, err = os.Stat(mdFilePath)
		require.NoError(err)

		keyB := core.DigestFixture().Hex()
		fB, err := store.Create(keyB, 10*memsize.KB)
		require.NoError(err)
		defer func() { require.NoError(fB.Close()) }()

		ok, err := store.GetMetadata(keyA, md, _dontIgnoreIncompleteFiles)
		require.Equal(os.ErrNotExist, err)
		require.False(ok)
		_, err = os.Stat(mdFilePath)
		require.True(errors.Is(err, os.ErrNotExist))
	})
}
