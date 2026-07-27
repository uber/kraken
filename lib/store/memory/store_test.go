package memory

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"regexp"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/memsize"
)

func newTestFile(t *testing.T, store *Store, size uint64) (f storelib.FileReadWriter, key string) {
	require := require.New(t)
	key = core.DigestFixture().Hex()
	f, err := store.Create(key, size)
	require.NoError(err)
	return f, key
}

func TestEviction(t *testing.T) {
	require := require.New(t)
	store, err := NewStore(25*memsize.KB, tally.NoopScope)
	require.NoError(err)
	// create 5 blobs - a, b, c, d, e with different sizes.
	a, aKey := newTestFile(t, store, 10*memsize.KB)
	b, bKey := newTestFile(t, store, 5*memsize.KB)
	c, cKey := newTestFile(t, store, 5*memsize.KB)
	_, dKey := newTestFile(t, store, 3*memsize.KB)
	e, eKey := newTestFile(t, store, 1*memsize.KB)

	require.Equal(24*memsize.KB, store.impl.size)
	// incomplete files cannot be evicted and adding 2KB would result in overreservation.
	_, err = store.Create(core.DigestFixture().Hex(), 2*memsize.KB)
	require.ErrorIs(err, ErrNoSpace)
	// start marking as complete in this specific order - c, b, a (MarkComplete resets access time).
	require.NoError(store.MarkComplete(cKey))
	require.NoError(store.MarkComplete(bKey))
	require.NoError(store.MarkComplete(aKey))
	// d is complete but its eviction is banned.
	require.NoError(store.MarkComplete(dKey))
	require.NoError(store.BanEviction(dKey))
	// e is banned from eviction before it even becomes complete.
	require.NoError(store.BanEviction(eKey))
	require.Equal(24*memsize.KB, store.impl.size)
	// Add f (4KB) which should evict c to make space, as d and e are unevictable and c was accessed last (the MarkComplete call).
	f, fKey := newTestFile(t, store, 4*memsize.KB)
	require.NoError(f.Close())
	require.NoError(store.MarkComplete(fKey))
	keys := store.List()
	require.NotContains(keys, cKey)
	_, err = c.Read(make([]byte, 5))
	require.ErrorIs(err, ErrEvicted)
	// new size == 23KB == 24KB - 5KB (c) + 4KB (f)
	require.Equal(23*memsize.KB, store.impl.size)

	// Add g (1KB), which will not evict anything
	g, gKey := newTestFile(t, store, 1*memsize.KB)
	require.NoError(g.Close())
	require.NoError(store.MarkComplete(gKey))
	require.Equal(24*memsize.KB, store.impl.size)

	// Add h (15KB), which evicts b and a:
	h, hKey := newTestFile(t, store, 15*memsize.KB)
	require.NoError(h.Close())
	require.NoError(store.MarkComplete(hKey))
	// size == 24KB == 24KB + 15KB (h) - 5KB (b) - 10KB (a)
	require.Equal(24*memsize.KB, store.impl.size)
	keys = store.List()
	require.NotContains(keys, bKey)
	_, err = b.Read(make([]byte, 5))
	require.ErrorIs(err, ErrEvicted)
	require.NotContains(keys, aKey)
	_, err = a.Read(make([]byte, 5))
	require.ErrorIs(err, ErrEvicted)

	// allow e to be evicted.
	require.NoError(store.MarkComplete(eKey))
	require.NoError(store.UnbanEviction(eKey))
	// eviction order (left-most is next to evict): f(4KB), g(1KB), h(15KB), e(1KB); d(3KB) is unevictable
	// we open g to change the order to f, h, e, g
	g, err = store.Open(gKey)
	require.NoError(err)
	require.NoError(g.Close())

	i, iKey := newTestFile(t, store, 5*memsize.KB)
	require.NoError(store.MarkComplete(iKey))
	require.NoError(i.Close())
	keys = store.List()
	require.NotContains(keys, fKey)
	_, err = f.Read(make([]byte, 5))
	require.ErrorIs(err, ErrEvicted)
	require.Equal(25*memsize.KB, store.impl.size)
	// eviction order: h(15KB), e(1KB), g(1KB), i(5KB); d(3KB) is unevictable

	j, jKey := newTestFile(t, store, 14*memsize.KB)
	require.NoError(j.Close())
	require.NoError(store.MarkComplete(jKey))
	keys = store.List()
	require.NotContains(keys, hKey)
	_, err = h.Read(make([]byte, 5))
	require.ErrorIs(err, ErrEvicted)
	require.Equal(24*memsize.KB, store.impl.size)
	// eviction order: e(1KB), g(1KB), i(5KB), j(14KB); d(3KB) is unevictable

	k, kKey := newTestFile(t, store, 2*memsize.KB)
	require.NoError(k.Close())
	require.NoError(store.MarkComplete(kKey))
	keys = store.List()
	require.NotContains(keys, eKey)
	_, err = e.Read(make([]byte, 5))
	require.ErrorIs(err, ErrEvicted)
	require.Equal(25*memsize.KB, store.impl.size)
	// eviction order: g(1KB), i(5KB), j(14KB), k(2KB); d(3KB) is unevictable

	l, lKey := newTestFile(t, store, 1*memsize.KB)
	require.NoError(store.MarkComplete(lKey))
	require.NoError(l.Close())
	keys = store.List()
	require.NotContains(keys, gKey)
	require.Equal(25*memsize.KB, store.impl.size)
	// eviction order: i(5KB), j(14KB), k(2KB), l(1KB); d(3KB) is unevictable

	require.NoError(store.Delete(iKey))
	require.NoError(store.Delete(jKey))
	require.NoError(store.Delete(lKey))
	// evictionOrder: k(2KB); d(3KB)
	require.Equal(5*memsize.KB, store.impl.size)
}

func TestEvictedBlobImmediatellyNotAvailable(t *testing.T) {
	require := require.New(t)
	store, err := NewStore(1*memsize.KB, tally.NoopScope)
	require.NoError(err)
	b, key := newTestFile(t, store, 1*memsize.KB)
	require.NoError(store.MarkComplete(key))

	_, newBlobKey := newTestFile(t, store, 1*memsize.KB)

	require.Equal([]string{newBlobKey}, store.List())
	buf := make([]byte, 10)

	n, err := b.Read(buf)
	require.True(n == 0)
	require.Equal(ErrEvicted, err)

	n, err = b.ReadAt(buf, 3)
	require.True(n == 0)
	require.Equal(ErrEvicted, err)

	seekN, err := b.Seek(10, io.SeekStart)
	require.True(seekN == 0)
	require.Equal(ErrEvicted, err)

	n, err = b.Write(buf)
	require.True(n == 0)
	require.Equal(ErrEvicted, err)

	n, err = b.WriteAt(buf, 3)
	require.True(n == 0)
	require.Equal(ErrEvicted, err)

	require.NoError(b.Cancel())
	require.NoError(b.Close())
	require.NoError(b.Commit())

	require.Equal(int64(0), b.Size())
}

func TestParallelAccessToSingleFile(t *testing.T) {
	require := require.New(t)
	store, err := NewStore(10*memsize.KB, tally.NoopScope)
	require.NoError(err)

	key := core.DigestFixture().Hex()
	// we purposefully underreport the size of the blob (real size is 50B) to test if the resizing logic is thread-safe.
	f, err := store.Create(key, 45*memsize.B)
	require.NoError(err)
	require.NoError(f.Close())
	// Spawn 5 routines in parallel that write and read to different parts of the file.
	var wg sync.WaitGroup
	wg.Add(5)

	type res struct {
		written, read []byte
		err           error
	}

	results := make([]res, 5)

	for idx := range 5 {
		go func(idx int64) {
			defer wg.Done()

			f, err := store.Open(key)
			if err != nil {
				results[idx].err = err
				return
			}
			pos := idx * 10
			writtenData := make([]byte, 10)
			for k := range writtenData {
				writtenData[k] = byte(idx)
			}
			_, err = f.WriteAt(writtenData, pos)
			if err != nil {
				results[idx].err = err
				return
			}

			readData := make([]byte, 10)
			_, err = f.ReadAt(readData, pos)
			if err != nil {
				results[idx].err = err
				return
			}
			err = f.Close()
			if err != nil {
				results[idx].err = err
				return
			}

			results[idx].read = readData
			results[idx].written = writtenData
		}(int64(idx))
	}

	wg.Wait()
	for idx := range 5 {
		require.NoError(results[idx].err)
		require.Equal(results[idx].written, results[idx].read)
	}

	require.NoError(store.MarkComplete(key))

	f, err = store.ScopeComplete().Open(key)
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
	store, err := NewStore(10*memsize.KB, tally.NoopScope)
	require.NoError(err)

	key := core.DigestFixture().Hex()
	data := []byte("Hello World")
	f, err := store.Create(key, uint64(len(data)))
	require.NoError(err)
	_, err = io.Copy(f, bytes.NewReader(data))
	require.NoError(err)
	require.NoError(f.Close())

	incompleteFile, err := store.Open(key)
	require.NoError(err)
	defer func() { require.NoError(incompleteFile.Close()) }()

	require.NoError(store.MarkComplete(key))

	completeFile, err := store.ScopeComplete().Open(key)
	require.NoError(err)
	defer func() { require.NoError(completeFile.Close()) }()

	incompleteFileData, err := io.ReadAll(incompleteFile)
	require.NoError(err)
	completeFileData, err := io.ReadAll(completeFile)
	require.NoError(err)

	require.Equal(data, incompleteFileData)
	require.Equal(data, completeFileData)
}

func TestMisreportedBlobSize(t *testing.T) {
	t.Run("user overreports size", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(25*memsize.KB, tally.NoopScope)
		require.NoError(err)

		// We pad the store's size to 1KB before all other operations to later show that size does not underflow when releasing memory.
		_, _ = newTestFile(t, store, 1*memsize.KB)

		f, key := newTestFile(t, store, 10*memsize.KB)
		data := make([]byte, 5*memsize.KB)
		_, err = rand.Read(data)
		require.NoError(err)
		_, err = io.Copy(f, bytes.NewReader(data))
		require.NoError(err)
		require.NoError(f.Close())

		f, err = store.Open(key)
		require.NoError(err)

		// io.ReadAll only returns the 5KB of actual data, despite us reporting the blob as 10KB. There is no 5KB of trailing space.
		readData, err := io.ReadAll(f)
		require.NoError(err)
		require.Equal(data, readData)

		// Stat returns the actual size of the blob.
		statSize, err := store.Stat(key)
		require.NoError(err)
		require.Equal(uint64(statSize), 5*memsize.KB)

		// The store always calculates its size based off the client-reported size, not the actual size.
		require.Equal(11*memsize.KB, store.impl.size)

		// Both memory reservation and memory release use the user-reported value, not the actual size of the blob.
		// Due to this consistency, the store's size is the same before adding and after deleting/evicting the blob.
		require.NoError(store.Delete(key))
		require.Equal(1*memsize.KB, store.impl.size)
	})

	t.Run("user underreports size", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(25*memsize.KB, tally.NoopScope)
		require.NoError(err)

		// We pad the store's size to 1KB before all other operations to later show that size does not underflow when releasing memory.
		_, _ = newTestFile(t, store, 1*memsize.KB)

		// We write 30KB to the file even though the store can only store 24KB. The store lets us do this and does not evict the blob.
		f, key := newTestFile(t, store, 24*memsize.KB)
		data := make([]byte, 30*memsize.KB)
		_, err = rand.Read(data)
		require.NoError(err)
		_, err = io.Copy(f, bytes.NewReader(data))
		require.NoError(err)
		require.NoError(f.Close())

		f, err = store.Open(key)
		require.NoError(err)

		// io.ReadAll only returns the 30KB of actual data, despite us reporting the blob as 24KB.
		// Despite the store initializing only 24KB of memory in Create, the slice is safely resized when writes exceed its length.
		readData, err := io.ReadAll(f)
		require.NoError(err)
		require.Equal(data, readData)

		// Stat returns the actual size of the blob.
		statSize, err := store.Stat(key)
		require.NoError(err)
		require.Equal(uint64(statSize), 30*memsize.KB)

		// The store always calculates its size based off the client-reported size, not the actual size.
		require.Equal(25*memsize.KB, store.impl.size)

		// Both memory reservation and memory release use the user-reported value, not the actual size of the blob.
		// Due to this consistency, the store's size is the same before adding and after deleting/evicting the blob.
		require.NoError(store.Delete(key))
		require.Equal(1*memsize.KB, store.impl.size)
	})
}

func TestDelete(t *testing.T) {
	t.Run("incomplete blob", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(f.Close())
		require.NoError(store.Delete(key))

		require.Empty(store.List())
		require.Equal(uint64(0), store.impl.size)
		require.Equal(0, len(store.impl.blobs))
	})
	t.Run("incomplete, unevictable blob", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(f.Close())
		require.NoError(store.BanEviction(key))

		require.NoError(store.Delete(key))

		require.Empty(store.List())
		require.Equal(uint64(0), store.impl.size)
		require.Equal(0, len(store.impl.blobs))
	})
	t.Run("complete blob", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(f.Close())
		require.NoError(store.MarkComplete(key))

		require.NoError(store.Delete(key))

		require.Empty(store.List())
		require.Equal(uint64(0), store.impl.size)
		require.Equal(0, len(store.impl.blobs))
	})
	t.Run("complete, unevictable blob", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(f.Close())
		require.NoError(store.MarkComplete(key))
		require.NoError(store.BanEviction(key))

		require.NoError(store.Delete(key))

		require.Empty(store.List())
		require.Equal(uint64(0), store.impl.size)
		require.Equal(0, len(store.impl.blobs))
	})
	t.Run("not found", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()

		err = store.Delete(key)
		require.ErrorIs(os.ErrNotExist, err)
	})
}

func TestMarkComplete(t *testing.T) {
	t.Run("incomplete blob", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)

		require.NoError(store.MarkComplete(key))

		require.Equal([]string{key}, store.ScopeComplete().List())
		require.Equal(uint64(100), store.impl.size)
		_, err = store.ScopeComplete().Open(key)
		require.NoError(err)
	})
	t.Run("incomplete blob with forbidden eviction", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 100*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 100)))
		require.NoError(err)
		require.NoError(store.BanEviction(key))

		require.NoError(store.MarkComplete(key))

		require.Equal([]string{key}, store.ScopeComplete().List())
		require.Equal(uint64(100), store.impl.size)
		_, err = store.ScopeComplete().Open(key)
		require.NoError(err)
	})
	t.Run("already complete blob", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
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
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
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
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()

		err = store.MarkComplete(key)
		require.ErrorIs(os.ErrNotExist, err)
	})
}

func TestStat(t *testing.T) {
	t.Run("complete blob", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 10*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 10)))
		require.NoError(err)
		require.NoError(store.MarkComplete(key))

		size, err := store.Stat(key)
		require.NoError(err)
		require.Equal(int64(10), size)
	})
	t.Run("complete, unevictable blob", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 10*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 10)))
		require.NoError(err)
		require.NoError(store.MarkComplete(key))
		require.NoError(store.BanEviction(key))

		size, err := store.Stat(key)
		require.NoError(err)
		require.Equal(int64(10), size)
	})
	t.Run("incomplete blob", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 10*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 10)))
		require.NoError(err)

		_, err = store.ScopeComplete().Stat(key)
		require.Equal(storelib.ErrOutOfScope, err)

		size, err := store.Stat(key)
		require.NoError(err)
		require.Equal(int64(10), size)
	})

	t.Run("incomplete, unevictable blob", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 10*memsize.B)
		require.NoError(err)
		defer func() { require.NoError(f.Close()) }()
		_, err = io.Copy(f, bytes.NewReader(make([]byte, 10)))
		require.NoError(err)
		require.NoError(store.BanEviction(key))

		size, err := store.Stat(key)
		require.NoError(err)
		require.Equal(int64(10), size)
	})
	t.Run("non-existent blob", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()

		size, err := store.Stat(key)
		require.ErrorIs(os.ErrNotExist, err)
		require.Equal(int64(0), size)
	})
}

func TestList(t *testing.T) {
	require := require.New(t)
	store, err := NewStore(10*memsize.KB, tally.NoopScope)
	require.NoError(err)

	require.Empty(store.ScopeComplete().List())
	require.Empty(store.ScopeComplete().List())

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

	wantRes := []string{completeBlobKey, unevictableCompleteBlobKey}
	res := store.ScopeComplete().List()
	require.ElementsMatch(wantRes, res)

	wantRes = []string{incompleteBlobKey, completeBlobKey, unevictableCompleteBlobKey, unevictableIncompleteBlobKey}
	res = store.List()
	require.ElementsMatch(wantRes, res)
}

func init() {
	metadata.Register(regexp.MustCompile("immovableMd"), &immovableMdFactory{})
}

// used for testing
type immovableMd struct{}
type immovableMdFactory struct{}

func (f *immovableMdFactory) Create(suffix string) metadata.Metadata { return &immovableMd{} }
func (mda *immovableMd) GetSuffix() string                           { return "immovableMd" }
func (mda *immovableMd) Movable() bool                               { return false }
func (mda *immovableMd) Serialize() ([]byte, error)                  { return nil, nil }
func (mda *immovableMd) Deserialize(b []byte) error                  { return nil }
func TestMetadata(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 10*memsize.KB)
		require.NoError(err)
		require.NoError(f.Close())

		mdStruct := core.MetaInfoFixture()
		writtenMd := metadata.NewTorrentMeta(mdStruct)
		err = store.SetMetadata(key, writtenMd)
		// asserts metadata is not included in LRU eviction calculation.
		require.NoError(err)

		var readMd metadata.TorrentMeta
		ok, err := store.GetMetadata(key, &readMd)
		require.NoError(err)
		require.True(ok)
		require.Equal(writtenMd.MetaInfo, readMd.MetaInfo)

		mdList, err := store.ListMetadata(key)
		require.NoError(err)
		require.Len(mdList, 1)
		require.Equal(writtenMd.GetSuffix(), mdList[0].GetSuffix())

		persistMd := metadata.NewPersist(false)
		require.NoError(store.SetMetadata(key, persistMd))
		var readPersistMd metadata.Persist
		ok, err = store.GetMetadata(key, &readPersistMd)
		require.NoError(err)
		require.True(ok)
		require.False(readPersistMd.Value)

		mdList, err = store.ListMetadata(key)
		require.NoError(err)
		require.ElementsMatch(
			[]string{writtenMd.GetSuffix(), persistMd.GetSuffix()},
			[]string{mdList[0].GetSuffix(), mdList[1].GetSuffix()},
		)

		require.NoError(store.DeleteMetadata(key, readMd.GetSuffix()))
		ok, err = store.GetMetadata(key, &readMd)
		require.NoError(err)
		require.False(ok)
		// deleting a second time should be a no-op.
		require.NoError(store.DeleteMetadata(key, readMd.GetSuffix()))

		mdList, err = store.ListMetadata(key)
		require.NoError(err)
		require.Len(mdList, 1)
		require.Equal(persistMd.GetSuffix(), mdList[0].GetSuffix())

		require.NoError(store.DeleteMetadata(key, persistMd.GetSuffix()))
		mdList, err = store.ListMetadata(key)
		require.NoError(err)
		require.Empty(mdList)
	})

	t.Run("non-existent blob", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		nonExistentKey := core.DigestFixture().Hex()
		mdStruct := core.MetaInfoFixture()
		md := metadata.NewTorrentMeta(mdStruct)

		err = store.SetMetadata(nonExistentKey, md)
		require.ErrorIs(os.ErrNotExist, err)

		ok, err := store.GetMetadata(nonExistentKey, md)
		require.ErrorIs(os.ErrNotExist, err)
		require.False(ok)

		_, err = store.ListMetadata(nonExistentKey)
		require.ErrorIs(os.ErrNotExist, err)

		err = store.DeleteMetadata(nonExistentKey, md.GetSuffix())
		require.ErrorIs(os.ErrNotExist, err)
	})

	t.Run("metadata does not change after marking a file as complete and/or evictable/unevictable", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 1*memsize.KB)
		require.NoError(err)
		require.NoError(f.Close())

		mdStruct := core.MetaInfoFixture()
		writtenMd := metadata.NewTorrentMeta(mdStruct)
		require.NoError(store.SetMetadata(key, writtenMd))

		var readMd metadata.TorrentMeta
		ok, err := store.ScopeComplete().GetMetadata(key, &readMd)
		require.Equal(storelib.ErrOutOfScope, err)
		// incomplete files are ignored
		require.False(ok)

		ok, err = store.GetMetadata(key, &readMd)
		require.NoError(err)
		require.True(ok)
		require.Equal(writtenMd.MetaInfo, readMd.MetaInfo)

		// Repeat the tests above for an unevictable file
		require.NoError(store.BanEviction(key))
		ok, err = store.ScopeComplete().GetMetadata(key, &readMd)
		require.Equal(storelib.ErrOutOfScope, err)
		// incomplete files are ignored
		require.False(ok)

		ok, err = store.GetMetadata(key, &readMd)
		require.NoError(err)
		require.True(ok)
		require.Equal(writtenMd.MetaInfo, readMd.MetaInfo)

		require.NoError(store.MarkComplete(key))
		ok, err = store.ScopeComplete().GetMetadata(key, &readMd)
		require.NoError(err)
		require.True(ok)
		require.Equal(writtenMd.MetaInfo, readMd.MetaInfo)
	})
	t.Run("metadata fully gone after blob is evicted", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		keyA := core.DigestFixture().Hex()
		fA, err := store.Create(keyA, 10*memsize.KB)
		require.NoError(err)
		require.NoError(fA.Close())
		require.NoError(store.MarkComplete(keyA))

		md := metadata.NewTorrentMeta(core.MetaInfoFixture())
		err = store.SetMetadata(keyA, md)
		require.NoError(err) // TODO

		readMd := metadata.TorrentMeta{}
		ok, err := store.GetMetadata(keyA, &readMd)
		require.NoError(err)
		require.True(ok)

		keyB := core.DigestFixture().Hex()
		fB, err := store.Create(keyB, 10*memsize.KB)
		require.NoError(err)
		defer func() { require.NoError(fB.Close()) }()

		ok, err = store.GetMetadata(keyA, md)
		require.ErrorIs(os.ErrNotExist, err)
		require.False(ok)
	})

	t.Run("immovable metadata is deleted when calling MarkComplete", func(t *testing.T) {
		require := require.New(t)
		store, err := NewStore(10*memsize.KB, tally.NoopScope)
		require.NoError(err)
		key := core.DigestFixture().Hex()
		f, err := store.Create(key, 10*memsize.KB)
		require.NoError(err)
		require.NoError(f.Close())

		md := &immovableMd{}
		require.NoError(store.SetMetadata(key, md))
		movableMd := metadata.NewTorrentMeta(core.MetaInfoFixture())
		require.NoError(store.SetMetadata(key, movableMd))

		require.NoError(store.MarkComplete(key))
		readMd := immovableMd{}
		ok, err := store.GetMetadata(key, &readMd)
		require.NoError(err)
		require.False(ok)

		readMovableMd := metadata.TorrentMeta{}
		ok, err = store.GetMetadata(key, &readMovableMd)
		require.NoError(err)
		require.True(ok)
	})
}

func TestScopes(t *testing.T) {
	require := require.New(t)
	store, err := NewStore(10*memsize.KB, tally.NoopScope)
	require.NoError(err)

	// Add a file to the store, fill it with data.
	f, key := newTestFile(t, store, 2*memsize.KB)
	data := make([]byte, 2*memsize.KB)
	_, err = rand.Read(data)
	require.NoError(err)
	_, err = io.Copy(f, bytes.NewReader(data))
	require.NoError(err)
	require.NoError(f.Close())

	// While incomplete, ScopeComplete's APIs reject the blob.
	_, err = store.ScopeComplete().Open(key)
	require.ErrorIs(err, storelib.ErrOutOfScope)
	_, err = store.ScopeComplete().Stat(key)
	require.ErrorIs(err, storelib.ErrOutOfScope)
	require.ErrorIs(store.ScopeComplete().BanEviction(key), storelib.ErrOutOfScope)
	require.ErrorIs(store.ScopeComplete().UnbanEviction(key), storelib.ErrOutOfScope)
	md := metadata.NewTorrentMeta(core.MetaInfoFixture())
	require.ErrorIs(store.ScopeComplete().SetMetadata(key, md), storelib.ErrOutOfScope)
	var readMd metadata.TorrentMeta
	ok, err := store.ScopeComplete().GetMetadata(key, &readMd)
	require.ErrorIs(err, storelib.ErrOutOfScope)
	require.False(ok)
	_, err = store.ScopeComplete().ListMetadata(key)
	require.ErrorIs(err, storelib.ErrOutOfScope)
	require.ErrorIs(store.ScopeComplete().DeleteMetadata(key, readMd.GetSuffix()), storelib.ErrOutOfScope)
	require.NotContains(store.ScopeComplete().List(), key)

	// The unscoped store's APIs work regardless of completeness.
	f, err = store.Open(key)
	require.NoError(err)
	readData, err := io.ReadAll(f)
	require.NoError(err)
	require.Equal(data, readData)
	require.NoError(f.Close())
	_, err = store.Stat(key)
	require.NoError(err)
	require.NoError(store.BanEviction(key))
	require.NoError(store.UnbanEviction(key))
	require.NoError(store.SetMetadata(key, md))
	ok, err = store.GetMetadata(key, &readMd)
	require.NoError(err)
	require.True(ok)
	require.Equal(md.MetaInfo, readMd.MetaInfo)
	mdList, err := store.ListMetadata(key)
	require.NoError(err)
	require.Len(mdList, 1)
	require.Equal(md.GetSuffix(), mdList[0].GetSuffix())
	require.NoError(store.DeleteMetadata(key, readMd.GetSuffix()))
	require.Contains(store.List(), key)

	// ScopeIncomplete's APIs also work, since the blob is (still) incomplete.
	f, err = store.ScopeIncomplete().Open(key)
	require.NoError(err)
	readData, err = io.ReadAll(f)
	require.NoError(err)
	require.Equal(data, readData)
	require.NoError(f.Close())
	_, err = store.ScopeIncomplete().Stat(key)
	require.NoError(err)
	require.NoError(store.ScopeIncomplete().BanEviction(key))
	require.NoError(store.ScopeIncomplete().UnbanEviction(key))
	require.NoError(store.ScopeIncomplete().SetMetadata(key, md))
	ok, err = store.ScopeIncomplete().GetMetadata(key, &readMd)
	require.NoError(err)
	require.True(ok)
	require.Equal(md.MetaInfo, readMd.MetaInfo)
	mdList, err = store.ScopeIncomplete().ListMetadata(key)
	require.NoError(err)
	require.Len(mdList, 1)
	require.Equal(md.GetSuffix(), mdList[0].GetSuffix())
	require.NoError(store.ScopeIncomplete().DeleteMetadata(key, readMd.GetSuffix()))
	require.Contains(store.ScopeIncomplete().List(), key)

	require.NoError(store.MarkComplete(key))

	// Now that the blob is complete, the roles reverse: ScopeIncomplete rejects it.
	_, err = store.ScopeIncomplete().Open(key)
	require.ErrorIs(err, storelib.ErrOutOfScope)
	_, err = store.ScopeIncomplete().Stat(key)
	require.ErrorIs(err, storelib.ErrOutOfScope)
	require.ErrorIs(store.ScopeIncomplete().BanEviction(key), storelib.ErrOutOfScope)
	require.ErrorIs(store.ScopeIncomplete().UnbanEviction(key), storelib.ErrOutOfScope)
	require.ErrorIs(store.ScopeIncomplete().SetMetadata(key, md), storelib.ErrOutOfScope)
	ok, err = store.ScopeIncomplete().GetMetadata(key, &readMd)
	require.ErrorIs(err, storelib.ErrOutOfScope)
	require.False(ok)
	_, err = store.ScopeIncomplete().ListMetadata(key)
	require.ErrorIs(err, storelib.ErrOutOfScope)
	require.ErrorIs(store.ScopeIncomplete().DeleteMetadata(key, readMd.GetSuffix()), storelib.ErrOutOfScope)
	require.NotContains(store.ScopeIncomplete().List(), key)

	// The unscoped store's APIs still work regardless of completeness.
	f, err = store.Open(key)
	require.NoError(err)
	readData, err = io.ReadAll(f)
	require.NoError(err)
	require.Equal(data, readData)
	require.NoError(f.Close())
	_, err = store.Stat(key)
	require.NoError(err)
	require.NoError(store.BanEviction(key))
	require.NoError(store.UnbanEviction(key))
	require.NoError(store.SetMetadata(key, md))
	ok, err = store.GetMetadata(key, &readMd)
	require.NoError(err)
	require.True(ok)
	require.Equal(md.MetaInfo, readMd.MetaInfo)
	mdList, err = store.ListMetadata(key)
	require.NoError(err)
	require.Len(mdList, 1)
	require.Equal(md.GetSuffix(), mdList[0].GetSuffix())
	require.NoError(store.DeleteMetadata(key, readMd.GetSuffix()))
	require.Contains(store.List(), key)

	// ScopeComplete's APIs now work, since the blob is complete.
	f, err = store.ScopeComplete().Open(key)
	require.NoError(err)
	readData, err = io.ReadAll(f)
	require.NoError(err)
	require.Equal(data, readData)
	require.NoError(f.Close())
	_, err = store.ScopeComplete().Stat(key)
	require.NoError(err)
	require.NoError(store.ScopeComplete().BanEviction(key))
	require.NoError(store.ScopeComplete().UnbanEviction(key))
	require.NoError(store.ScopeComplete().SetMetadata(key, md))
	ok, err = store.ScopeComplete().GetMetadata(key, &readMd)
	require.NoError(err)
	require.True(ok)
	require.Equal(md.MetaInfo, readMd.MetaInfo)
	mdList, err = store.ScopeComplete().ListMetadata(key)
	require.NoError(err)
	require.Len(mdList, 1)
	require.Equal(md.GetSuffix(), mdList[0].GetSuffix())
	require.NoError(store.ScopeComplete().DeleteMetadata(key, readMd.GetSuffix()))
	require.Contains(store.ScopeComplete().List(), key)
}

func TestScopesDelete(t *testing.T) {
	require := require.New(t)
	store, err := NewStore(10*memsize.KB, tally.NoopScope)
	require.NoError(err)

	f, key := newTestFile(t, store, 1*memsize.KB)
	require.NoError(f.Close())
	require.ErrorIs(store.ScopeComplete().Delete(key), storelib.ErrOutOfScope)
	require.NoError(store.ScopeIncomplete().Delete(key))
	_, err = store.Stat(key)
	require.ErrorIs(err, os.ErrNotExist)

	f, key = newTestFile(t, store, 1*memsize.KB)
	require.NoError(f.Close())
	require.NoError(store.MarkComplete(key))
	require.ErrorIs(store.ScopeIncomplete().Delete(key), storelib.ErrOutOfScope)
	require.NoError(store.ScopeComplete().Delete(key))
	_, err = store.Stat(key)
	require.ErrorIs(err, os.ErrNotExist)

	f, key = newTestFile(t, store, 1*memsize.KB)
	require.NoError(f.Close())
	require.NoError(store.Delete(key))

	f, key = newTestFile(t, store, 1*memsize.KB)
	require.NoError(f.Close())
	require.NoError(store.MarkComplete(key))
	require.NoError(store.Delete(key))
}
