package tiered

import (
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/lib/store/memory"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/memsize"
	"github.com/uber/kraken/utils/testutil"
)

const _testBlobSize = 1 * memsize.KB

func newDiskConfig(rootDir string, capacity uint64) *disk.Config {
	return &disk.Config{CapacityBytes: capacity, RootDir: rootDir}
}

func newTestStore(t *testing.T, diskCapacity, memCapacity uint64, numWorkers int) (s *Store, rootDir string) {
	rootDir = t.TempDir()
	s, err := NewStore(newDiskConfig(rootDir, diskCapacity), memCapacity, numWorkers, tally.NoopScope)
	require.NoError(t, err)
	return s, rootDir
}

func randomData(t *testing.T, size uint64) []byte {
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	return data
}

// createAndWrite creates a blob through the public API and writes data to it.
// Whether the blob lands in mem or disk depends on the store's configured capacities.
func createAndWrite(t *testing.T, s *Store, size uint64) (key string, data []byte) {
	key = core.DigestFixture().Hex()
	data = randomData(t, size)
	f, err := s.Create(key, size)
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return key, data
}

func waitForComplete(t *testing.T, s *Store, key string) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		_, ok := s.ScopeComplete().Has(key)
		return ok
	})
	require.NoError(t, err)
}

// waitForFlushed waits until key has been fully flushed from mem to disk.
func waitForFlushed(t *testing.T, s *Store, key string) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		_, ok := s.impl.disk.ScopeComplete().Has(key)
		return ok
	})
	require.NoError(t, err)
}

// waitForFlusherIdle waits until the flusher is no longer tracking key.
func waitForFlusherIdle(t *testing.T, s *Store, key string) {
	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		s.impl.flusher.mu.Lock()
		defer s.impl.flusher.mu.Unlock()
		_, tracked := s.impl.flusher.blobs[key]
		return !tracked
	})
	require.NoError(t, err)
}

// The helpers below deterministically put a blob into one of the store's six
// possible lifecycle states, using the real public API for all of them.

func seedOnlyMemIncomplete(t *testing.T, s *Store) (key string, data []byte) {
	return createAndWrite(t, s, _testBlobSize)
}

func seedMemCompleteEnqueuedNotOnDisk(t *testing.T, s *Store) (key string, data []byte) {
	key, data = createAndWrite(t, s, _testBlobSize)

	reached := make(chan struct{})
	blocked := make(chan struct{})
	old := memOpen
	memOpen = func(mem *memory.Store, k string) (*memory.File, error) {
		if k == key {
			close(reached)
			<-blocked
		}
		return old(mem, k)
	}
	t.Cleanup(func() {
		memOpen = old
		close(blocked)
		waitForFlusherIdle(t, s, key)
	})

	require.NoError(t, s.MarkComplete(key))
	timedOut := false
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		timedOut = true
	}
	require.False(t, timedOut, "timed out waiting for flusher worker to reach memOpen")
	return key, data
}

func seedMemCompleteDiskIncomplete(t *testing.T, s *Store) (key string, data []byte) {
	key, data = createAndWrite(t, s, _testBlobSize)

	reached := make(chan struct{})
	blocked := make(chan struct{})
	old := ioCopy
	ioCopy = func(dst io.Writer, src io.Reader) (int64, error) {
		close(reached)
		<-blocked
		return old(dst, src)
	}
	t.Cleanup(func() {
		ioCopy = old
		close(blocked)
		waitForFlusherIdle(t, s, key)
	})

	require.NoError(t, s.MarkComplete(key))
	timedOut := false
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		timedOut = true
	}
	require.False(t, timedOut, "timed out waiting for flusher worker to reach ioCopy")
	return key, data
}

func seedBothComplete(t *testing.T, s *Store) (key string, data []byte) {
	key, data = createAndWrite(t, s, _testBlobSize)
	require.NoError(t, s.MarkComplete(key))
	waitForFlushed(t, s, key)
	return key, data
}

func newStateOnlyMemIncomplete(t *testing.T) (s *Store, key string, data []byte) {
	s, _ = newTestStore(t, 10*memsize.KB, 10*memsize.KB, 1)
	key, data = seedOnlyMemIncomplete(t, s)
	return s, key, data
}

func newStateMemCompleteEnqueuedNotOnDisk(t *testing.T) (s *Store, key string, data []byte) {
	s, _ = newTestStore(t, 10*memsize.KB, 10*memsize.KB, 1)
	key, data = seedMemCompleteEnqueuedNotOnDisk(t, s)
	return s, key, data
}

func newStateMemCompleteDiskIncomplete(t *testing.T) (s *Store, key string, data []byte) {
	s, _ = newTestStore(t, 10*memsize.KB, 10*memsize.KB, 1)
	key, data = seedMemCompleteDiskIncomplete(t, s)
	return s, key, data
}

func newStateBothComplete(t *testing.T) (s *Store, key string, data []byte) {
	s, _ = newTestStore(t, 10*memsize.KB, 10*memsize.KB, 1)
	key, data = seedBothComplete(t, s)
	return s, key, data
}

// newStateDiskOnlyIncomplete uses a mem capacity of 1 byte so the real Create
// call overflows to disk via the genuine memory.ErrNoSpace fallback path.
func newStateDiskOnlyIncomplete(t *testing.T) (s *Store, key string, data []byte) {
	s, _ = newTestStore(t, 10*memsize.KB, 1, 1)
	key, data = createAndWrite(t, s, _testBlobSize)
	return s, key, data
}

func newStateDiskOnlyComplete(t *testing.T) (s *Store, key string, data []byte) {
	s, key, data = newStateDiskOnlyIncomplete(t)
	require.NoError(t, s.MarkComplete(key))
	return s, key, data
}

type stateBuilder struct {
	name  string
	build func(t *testing.T) (s *Store, key string, data []byte)
}

func allStates() []stateBuilder {
	return []stateBuilder{
		{"only in mem, incomplete", newStateOnlyMemIncomplete},
		{"complete in mem, enqueued, not yet on disk", newStateMemCompleteEnqueuedNotOnDisk},
		{"complete in mem, incomplete on disk", newStateMemCompleteDiskIncomplete},
		{"complete in both mem and disk", newStateBothComplete},
		{"only on disk, incomplete", newStateDiskOnlyIncomplete},
		{"only on disk, complete", newStateDiskOnlyComplete},
	}
}

func TestNewStore_RejectsRebootIncompleteBlobs(t *testing.T) {
	require := require.New(t)
	config := newDiskConfig(t.TempDir(), 10*memsize.KB)
	config.RebootIncompleteBlobs = true

	_, err := NewStore(config, 10*memsize.KB, 1, tally.NoopScope)
	require.EqualError(err, "tiered.Store does not support RebootIncompleteBlobs, as it can leak files. Use disk.Store if you need persistence for incomplete blobs")
}

func TestNewStore_RejectsNonPositiveNumWorkers(t *testing.T) {
	require := require.New(t)
	config := newDiskConfig(t.TempDir(), 10*memsize.KB)

	_, err := NewStore(config, 10*memsize.KB, 0, tally.NoopScope)
	require.EqualError(err, "numWorkers must be at least 1, otherwise blobs would never get flushed from mem to disk")

	_, err = NewStore(config, 10*memsize.KB, -1, tally.NoopScope)
	require.EqualError(err, "numWorkers must be at least 1, otherwise blobs would never get flushed from mem to disk")
}

func TestStore_APICorrectnessAcrossStates(t *testing.T) {
	for _, sb := range allStates() {
		t.Run(sb.name, func(t *testing.T) {
			t.Run("Open", func(t *testing.T) {
				require := require.New(t)
				s, key, data := sb.build(t)
				f, err := s.Open(key)
				require.NoError(err)
				got, err := io.ReadAll(f)
				require.NoError(err)
				require.Equal(data, got)
				require.NoError(f.Close())
			})
			t.Run("Has", func(t *testing.T) {
				require := require.New(t)
				s, key, _ := sb.build(t)
				inStore, _ := s.Has(key)
				require.True(inStore)
			})
			t.Run("Stat", func(t *testing.T) {
				require := require.New(t)
				s, key, data := sb.build(t)
				size, err := s.Stat(key)
				require.NoError(err)
				require.Equal(int64(len(data)), size)
			})
			t.Run("MarkComplete", func(t *testing.T) {
				require := require.New(t)
				s, key, _ := sb.build(t)
				_, wasAlreadyComplete := s.ScopeComplete().Has(key)
				require.NoError(s.MarkComplete(key))
				waitForComplete(t, s, key)
				if !wasAlreadyComplete {
					waitForFlushed(t, s, key)
				}
				require.NoError(s.MarkComplete(key))
			})
			t.Run("Delete", func(t *testing.T) {
				require := require.New(t)
				s, key, _ := sb.build(t)
				require.NoError(s.Delete(key))
				_, ok := s.Has(key)
				require.False(ok)
				_, inMem := s.impl.mem.Has(key)
				require.False(inMem)
				_, inDisk := s.impl.disk.Has(key)
				require.False(inDisk)
			})
			t.Run("List", func(t *testing.T) {
				require := require.New(t)
				s, key, _ := sb.build(t)
				require.Contains(s.List(), key)
			})
		})
	}
}

func TestStore_ScopingAcrossStates(t *testing.T) {
	completeStates := []stateBuilder{
		{name: "complete in mem, enqueued, not yet on disk", build: newStateMemCompleteEnqueuedNotOnDisk},
		{name: "complete in mem, incomplete on disk", build: newStateMemCompleteDiskIncomplete},
		{name: "complete in both mem and disk", build: newStateBothComplete},
		{name: "only on disk, complete", build: newStateDiskOnlyComplete},
	}
	incompleteStates := []stateBuilder{
		{name: "only in mem, incomplete", build: newStateOnlyMemIncomplete},
		{name: "only on disk, incomplete", build: newStateDiskOnlyIncomplete},
	}

	for _, sb := range completeStates {
		t.Run(sb.name, func(t *testing.T) {
			require := require.New(t)
			s, key, data := sb.build(t)

			_, ok := s.ScopeComplete().Has(key)
			require.True(ok)
			_, ok = s.ScopeIncomplete().Has(key)
			require.False(ok)

			f, err := s.ScopeComplete().Open(key)
			require.NoError(err)
			got, err := io.ReadAll(f)
			require.NoError(err)
			require.Equal(data, got)
			require.NoError(f.Close())

			_, err = s.ScopeIncomplete().Open(key)
			require.ErrorIs(err, storelib.ErrOutOfScope)

			_, err = s.ScopeComplete().Stat(key)
			require.NoError(err)
			_, err = s.ScopeIncomplete().Stat(key)
			require.ErrorIs(err, storelib.ErrOutOfScope)

			require.ErrorIs(s.ScopeIncomplete().Delete(key), storelib.ErrOutOfScope)
		})
	}

	for _, sb := range incompleteStates {
		t.Run(sb.name, func(t *testing.T) {
			require := require.New(t)
			s, key, data := sb.build(t)

			_, ok := s.ScopeIncomplete().Has(key)
			require.True(ok)
			_, ok = s.ScopeComplete().Has(key)
			require.False(ok)

			f, err := s.ScopeIncomplete().Open(key)
			require.NoError(err)
			got, err := io.ReadAll(f)
			require.NoError(err)
			require.Equal(data, got)
			require.NoError(f.Close())

			_, err = s.ScopeComplete().Open(key)
			require.ErrorIs(err, storelib.ErrOutOfScope)

			_, err = s.ScopeIncomplete().Stat(key)
			require.NoError(err)
			_, err = s.ScopeComplete().Stat(key)
			require.ErrorIs(err, storelib.ErrOutOfScope)

			require.ErrorIs(s.ScopeComplete().Delete(key), storelib.ErrOutOfScope)
		})
	}
}

func TestStore_List_DedupDuringFlush(t *testing.T) {
	require := require.New(t)
	s, key, _ := newStateMemCompleteDiskIncomplete(t)

	require.NotContains(s.ScopeIncomplete().List(), key)

	all := s.List()
	require.Len(all, 1)
	require.Contains(all, key)

	require.Equal([]string{key}, s.ScopeComplete().List())
}

func TestStore_CrashRecovery(t *testing.T) {
	require := require.New(t)
	// 2 workers: states 2 and 3 each block a flusher worker via the seams.
	s, rootDir := newTestStore(t, 10*memsize.KB, 10*memsize.KB, 2)

	// Seed state 4 first while both workers are free.
	bothCompleteKey, bothCompleteData := seedBothComplete(t, s)

	onlyMemIncompleteKey, _ := seedOnlyMemIncomplete(t, s)
	enqueuedKey, _ := seedMemCompleteEnqueuedNotOnDisk(t, s)
	memCompleteDiskIncompleteKey, _ := seedMemCompleteDiskIncomplete(t, s)

	// States 5 and 6 write directly to the disk sub-store. The real API's
	// overflow-to-disk path (memory.ErrNoSpace) is tested by the matrix via
	// newStateDiskOnlyIncomplete/Complete; here we bypass it because the
	// shared store has enough mem capacity for the other states.
	diskOnlyIncompleteKey := core.DigestFixture().Hex()
	diskF, err := s.impl.disk.Create(diskOnlyIncompleteKey, _testBlobSize)
	require.NoError(err)
	require.NoError(diskF.Close())

	diskOnlyCompleteKey := core.DigestFixture().Hex()
	diskOnlyCompleteData := randomData(t, _testBlobSize)
	diskF, err = s.impl.disk.Create(diskOnlyCompleteKey, _testBlobSize)
	require.NoError(err)
	_, err = diskF.Write(diskOnlyCompleteData)
	require.NoError(err)
	require.NoError(diskF.Close())
	require.NoError(s.impl.disk.MarkComplete(diskOnlyCompleteKey))

	restarted, err := NewStore(newDiskConfig(rootDir, 10*memsize.KB), 10*memsize.KB, 1, tally.NoopScope)
	require.NoError(err)

	for _, discardedKey := range []string{
		onlyMemIncompleteKey,
		enqueuedKey,
		memCompleteDiskIncompleteKey,
		diskOnlyIncompleteKey,
	} {
		_, ok := restarted.Has(discardedKey)
		require.False(ok, "key %q should have been discarded on restart", discardedKey)
	}

	f, err := restarted.Open(bothCompleteKey)
	require.NoError(err)
	got, err := io.ReadAll(f)
	require.NoError(err)
	require.Equal(bothCompleteData, got)
	require.NoError(f.Close())

	f, err = restarted.Open(diskOnlyCompleteKey)
	require.NoError(err)
	got, err = io.ReadAll(f)
	require.NoError(err)
	require.Equal(diskOnlyCompleteData, got)
	require.NoError(f.Close())
}

func TestStore_MetadataLifecycleAcrossStates(t *testing.T) {
	for _, sb := range allStates() {
		t.Run(sb.name, func(t *testing.T) {
			require := require.New(t)
			s, key, _ := sb.build(t)

			md := metadata.NewTorrentMeta(core.MetaInfoFixture())
			require.NoError(s.SetMetadata(key, md))

			var readMd metadata.TorrentMeta
			ok, err := s.GetMetadata(key, &readMd)
			require.NoError(err)
			require.True(ok)
			require.Equal(md.MetaInfo, readMd.MetaInfo)

			require.NoError(s.DeleteMetadata(key, readMd.GetSuffix()))
			ok, err = s.GetMetadata(key, &readMd)
			require.NoError(err)
			require.False(ok)
		})
	}
}

func TestStore_SetMetadataOnIncompleteBlob_IsNoOpForFlushing(t *testing.T) {
	require := require.New(t)
	s, _ := newTestStore(t, 10*memsize.KB, 10*memsize.KB, 1)
	key, _ := createAndWrite(t, s, _testBlobSize)

	md := metadata.NewTorrentMeta(core.MetaInfoFixture())
	require.NoError(s.SetMetadata(key, md))

	s.impl.flusher.mu.Lock()
	_, tracked := s.impl.flusher.blobs[key]
	s.impl.flusher.mu.Unlock()
	require.False(tracked)

	_, inDisk := s.impl.disk.Has(key)
	require.False(inDisk)
}
