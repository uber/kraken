package store

import (
	"bytes"
	"io"
	"os"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/memsize"
)

func testStore(t *testing.T, capacity uint64) (res *DiskStore, rootDir string) {
	rootDir, err := os.MkdirTemp("/tmp", "kraken-disk-store")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(rootDir) })

	store, err := NewDiskStore(capacity, rootDir)
	require.NoError(t, err)
	return store, rootDir
}

func TestOpen(t *testing.T) {
	t.Skip("TODO")
	// Assert that reading a file before it's committed doesn't work.
}

func TestDiskStore(t *testing.T) {
	require := require.New(t)
	store, _ := testStore(t, 10*memsize.KB)

	digests := []core.Digest{}
	for i := range 10 {
		digest := core.DigestFixture()
		digests = append(digests, digest)
		writer, err := store.Create(digest.Hex(), memsize.KB)
		require.NoError(err)

		data := make([]byte, memsize.KB)
		for k := range data {
			data[k] = byte(i + 1)
		}
		n, err := io.Copy(writer, bytes.NewReader(make([]byte, memsize.KB)))
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

	// now test that LRU logic works - make sure that the 1 that is committed gets evicted.
	digest = core.DigestFixture()
	writer, err = store.Create(digest.Hex(), memsize.KB)
	require.NoError(err)
	reader, err = store.Open(digests[0].Hex(), true)
	require.Equal(err, os.ErrNotExist)
	require.Nil(reader)
}

func TestEviction(t *testing.T) {
	// order - when 1) complete evictable, 2) incomplete evictable, 3) incomplete unevictable, and 4) complete unevictable blobs are in store.
	// order - make sure that marking a blob as complete considers it as most recently used. same for `EnableEviction`.
	// correctness - does not evict unevictable/incomplete blobs no matter what.
	// new test - eviction works under parallel access
	// assert actual size of disk directory changes as expected throuhgout the whole process
	t.Skip("TODO")
}

func TestCrashRecovery(t *testing.T) {
	t.Skip("TODO - implement")
}

func TestParallelAccessToSingleFile(t *testing.T) {
	require := require.New(t)
	store, _ := testStore(t, 10*memsize.KB)

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
	store, _ := testStore(t, 10*memsize.KB)

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
	t.Run("uncommitted blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		w, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(w, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)

		require.NoError(store.Delete(key))

		require.Empty(store.List(true))
		require.Equal(uint64(0), store.size)
	})
	t.Run("uncommitted blob with forbidden eviction", func(t *testing.T) {
		t.Skip("TODO - test this once ForbidEviction is implemented")
	})
	t.Run("committed, evictable blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		w, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(w, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(store.MarkComplete(key))

		require.NoError(store.Delete(key))

		require.Empty(store.List(true))
		require.Equal(uint64(0), store.size)
		_, err = store.Open(key, true)
		require.Equal(os.ErrNotExist, err)
	})
	t.Run("committed, unevictable blob", func(t *testing.T) {
		t.Skip("TODO - test this once unevictable blobs are implemented")
	})
	t.Run("not found", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()

		err := store.Delete(key)
		require.Equal(os.ErrNotExist, err)
	})
}

func TestMarkComplete(t *testing.T) {
	t.Run("uncommitted blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		w, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(w, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)

		require.NoError(store.MarkComplete(key))

		require.Equal([]string{key}, store.List(true))
		require.Equal(uint64(100), store.size)
		_, err = store.Open(key, true)
		require.NoError(err)
	})
	t.Run("uncommitted blob with forbidden eviction", func(t *testing.T) {
		t.Skip("TODO - test this once ForbidEviction is implemented")
	})
	t.Run("already committed blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		w, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(w, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(store.MarkComplete(key))

		err = store.MarkComplete(key)
		require.EqualError(err, "blob is not in incomplete state")
	})
	t.Run("already committed blob with forbidden eviction", func(t *testing.T) {
		t.Skip("TODO - test this once ForbidEviction is implemented")
	})
	t.Run("not found", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()

		err := store.MarkComplete(key)
		require.EqualError(err, "blob is not in incomplete state")
	})
}

func TestStat(t *testing.T) {
	t.Run("committed, evictable blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		w, err := store.Create(key, 10*memsize.B)
		require.NoError(err)
		_, err = io.Copy(w, bytes.NewReader(make([]byte, 10)))
		require.NoError(err)
		require.NoError(store.MarkComplete(key))

		fInfo, err := store.Stat(key, true)
		require.NoError(err)

		require.False(fInfo.IsDir())
		require.WithinDuration(time.Now(), fInfo.ModTime(), 500*time.Millisecond)
		require.Equal(_blobFileName, fInfo.Name())
		require.Equal(int64(10), fInfo.Size())
	})
	t.Run("committed, unevictable blob", func(t *testing.T) {
		t.Skip("TODO - test this once unevictable blobs are implemented")
	})
	t.Run("uncommitted blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		w, err := store.Create(key, 10*memsize.B)
		require.NoError(err)
		_, err = io.Copy(w, bytes.NewReader(make([]byte, 10)))
		require.NoError(err)

		fInfo, err := store.Stat(key, true)
		require.Equal(os.ErrNotExist, err)
		require.Nil(fInfo)
	})
	t.Run("non-existent blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()

		_, err := store.Stat(key, true)
		require.Equal(os.ErrNotExist, err)
	})
}

func TestList(t *testing.T) {
	require := require.New(t)
	store, _ := testStore(t, 10*memsize.KB)

	require.Empty(store.List(true))

	uncommittedBlobKey := core.DigestFixture().Hex()
	w, err := store.Create(uncommittedBlobKey, 10*memsize.B)
	require.NoError(err)
	_, err = io.Copy(w, bytes.NewReader(make([]byte, 10)))
	require.NoError(err)
	committedBlobKey := core.DigestFixture().Hex()
	w, err = store.Create(committedBlobKey, 10*memsize.B)
	require.NoError(err)
	_, err = io.Copy(w, bytes.NewReader(make([]byte, 10)))
	require.NoError(err)
	require.NoError(store.MarkComplete(committedBlobKey))
	// TODO - add an unevictable blob here once that's implemented

	require.Equal([]string{committedBlobKey}, store.List(true))
}

func TestPathing(t *testing.T) {
	require := require.New(t)
	store, rootDir := testStore(t, 10*memsize.KB)

	key := "8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"
	committed := false
	dirPath := store.dirPath(key, committed)
	wantDirPath := rootDir + "/incomplete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"
	require.Equal(wantDirPath, dirPath)
	blobPath := store.blobPath(key, committed)
	wantBlobPath := rootDir + "/incomplete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/data"
	require.Equal(wantBlobPath, blobPath)

	committed = true
	dirPath = store.dirPath(key, committed)
	wantDirPath = rootDir + "/complete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"
	require.Equal(wantDirPath, dirPath)
	blobPath = store.blobPath(key, committed)
	wantBlobPath = rootDir + "/complete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/data"
	require.Equal(wantBlobPath, blobPath)
}

func TestForbidEviction(t *testing.T) {
	t.Skip("TODO")
	// unevictable blobs can neither be evicted nor deleted. remarking as evictable works but resets access time.
}
