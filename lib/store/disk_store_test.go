package store

import (
	"bytes"
	"io"
	"os"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/memsize"
)

/*
tests to do:
 - evictable and uncommitted blobs cannot be evicted automatically (StartWrite)
 - some blobs can get evicted but still error is returned eventually
 - do some eviction and measure actual disk usage of the directory, not just the tracked size. Make sure it matches expectations (test whether closing the file and thus releasing the fd reduces this usage).
 - restart recovery works (simulate a crash)
 - unevictable blobs can be fetched and cannot be evicted
 - unevictable blobs can be unmarked as unevictable (and thus can get GC-d)
 - cover all error cases in the code
 - blob is still readable after eviction (linux is holding the FD open)
 - trying to read a file before it's committed doesn't work.
*/

func testStore(t *testing.T, capacity uint64) (res *DiskStore, rootDir string) {
	rootDir, err := os.MkdirTemp("/tmp", "kraken-disk-store")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(rootDir) })

	store, err := NewDiskStore(capacity, rootDir)
	require.NoError(t, err)
	return store, rootDir
}

func TestDiskStore(t *testing.T) {
	require := require.New(t)
	store, _ := testStore(t, 10*memsize.KB)

	digests := []core.Digest{}
	for i := range 10 {
		digest := core.DigestFixture()
		digests = append(digests, digest)
		writer, err := store.StartWrite(digest.Hex(), memsize.KB)
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
	writer, err := store.StartWrite(digest.Hex(), memsize.B)
	require.EqualError(err, "reserve space: cannot evict enough, the unevictable/uncommitted blobs are using up all the space")
	require.Nil(writer)

	reader, err := store.Get(digests[0].Hex())
	require.Equal(err, os.ErrNotExist)
	require.Nil(reader)

	require.NoError(store.CommitWrite(digests[0].Hex()))
	reader, err = store.Get(digests[0].Hex())
	require.NoError(err)
	wantData := make([]byte, memsize.KB)
	for k := range wantData {
		wantData[k] = byte(1)
	}
	require.NoError(iotest.TestReader(reader, make([]byte, memsize.KB)))

	// now test that LRU logic works - make sure that the 1 that is committed gets evicted.
	digest = core.DigestFixture()
	writer, err = store.StartWrite(digest.Hex(), memsize.KB)
	require.NoError(err)
	reader, err = store.Get(digests[0].Hex())
	require.Equal(err, os.ErrNotExist)
	require.Nil(reader)
}

func TestDelete(t *testing.T) {
	t.Run("uncommitted blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		w, err := store.StartWrite(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(w, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)

		require.NoError(store.Delete(key))

		require.Empty(store.List())
		require.Equal(uint64(0), store.size)
	})
	t.Run("uncommitted blob with forbidden eviction", func(t *testing.T) {
		t.Skip("TODO - test this once ForbidEviction is implemented")
	})
	t.Run("committed, evictable blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		w, err := store.StartWrite(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(w, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(store.CommitWrite(key))

		require.NoError(store.Delete(key))

		require.Empty(store.List())
		require.Equal(uint64(0), store.size)
		_, err = store.Get(key)
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

func TestCommitWrite(t *testing.T) {
	t.Run("uncommitted blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		w, err := store.StartWrite(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(w, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)

		require.NoError(store.CommitWrite(key))

		require.Equal([]string{key}, store.List())
		require.Equal(uint64(100), store.size)
		_, err = store.Get(key)
		require.NoError(err)
	})
	t.Run("uncommitted blob with forbidden eviction", func(t *testing.T) {
		t.Skip("TODO - test this once ForbidEviction is implemented")
	})
	t.Run("already committed blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		w, err := store.StartWrite(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(w, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(store.CommitWrite(key))

		err = store.CommitWrite(key)
		require.EqualError(err, "blob is not in uncommitted state")
	})
	t.Run("already committed blob with forbidden eviction", func(t *testing.T) {
		t.Skip("TODO - test this once ForbidEviction is implemented")
	})
	t.Run("not found", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()

		err := store.CommitWrite(key)
		require.EqualError(err, "blob is not in uncommitted state")
	})
}

func TestStat(t *testing.T) {
	t.Run("committed, evictable blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()
		w, err := store.StartWrite(key, 10*memsize.B)
		require.NoError(err)
		_, err = io.Copy(w, bytes.NewReader(make([]byte, 10)))
		require.NoError(err)
		require.NoError(store.CommitWrite(key))

		fInfo, err := store.Stat(key)
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
		w, err := store.StartWrite(key, 10*memsize.B)
		require.NoError(err)
		_, err = io.Copy(w, bytes.NewReader(make([]byte, 10)))
		require.NoError(err)

		fInfo, err := store.Stat(key)
		require.Equal(os.ErrNotExist, err)
		require.Nil(fInfo)
	})
	t.Run("non-existent blob", func(t *testing.T) {
		require := require.New(t)
		store, _ := testStore(t, 10*memsize.KB)
		key := core.DigestFixture().Hex()

		_, err := store.Stat(key)
		require.Equal(os.ErrNotExist, err)
	})
}

func TestList(t *testing.T) {
	require := require.New(t)
	store, _ := testStore(t, 10*memsize.KB)

	require.Empty(store.List())

	uncommittedBlobKey := core.DigestFixture().Hex()
	w, err := store.StartWrite(uncommittedBlobKey, 10*memsize.B)
	require.NoError(err)
	_, err = io.Copy(w, bytes.NewReader(make([]byte, 10)))
	require.NoError(err)
	committedBlobKey := core.DigestFixture().Hex()
	w, err = store.StartWrite(committedBlobKey, 10*memsize.B)
	require.NoError(err)
	_, err = io.Copy(w, bytes.NewReader(make([]byte, 10)))
	require.NoError(err)
	require.NoError(store.CommitWrite(committedBlobKey))
	// TODO - add an unevictable blob here once that's implemented

	require.Equal([]string{committedBlobKey}, store.List())
}

func TestPathing(t *testing.T) {
	require := require.New(t)
	store, rootDir := testStore(t, 10*memsize.KB)

	key := "8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"
	committed := false
	dirPath := store.dirPath(key, committed)
	wantDirPath := rootDir + "/uncommitted/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"
	require.Equal(wantDirPath, dirPath)
	blobPath := store.blobPath(key, committed)
	wantBlobPath := rootDir + "/uncommitted/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/data"
	require.Equal(wantBlobPath, blobPath)

	committed = true
	dirPath = store.dirPath(key, committed)
	wantDirPath = rootDir + "/committed/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"
	require.Equal(wantDirPath, dirPath)
	blobPath = store.blobPath(key, committed)
	wantBlobPath = rootDir + "/committed/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/data"
	require.Equal(wantBlobPath, blobPath)
}
