package store

import (
	"bytes"
	"io"
	"os"
	"testing"
	"testing/iotest"

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

func TestDiskStore(t *testing.T) {
	require := require.New(t)
	rootDir, err := os.MkdirTemp("/tmp", "kraken-disk-store")
	require.NoError(err)
	defer os.RemoveAll(rootDir)

	capacity := 10 * memsize.KB
	store, err := NewDiskStore(capacity, rootDir)
	require.NoError(err)

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

func TestPathing(t *testing.T) {
	require := require.New(t)
	rootDir, err := os.MkdirTemp("/tmp", "kraken-disk-store")
	require.NoError(err)
	defer os.RemoveAll(rootDir)

	capacity := 10 * memsize.KB
	store, err := NewDiskStore(capacity, rootDir)
	require.NoError(err)

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
