package tiered

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/memsize"
	"github.com/uber/kraken/utils/testutil"
)

func TestFlusher_MetadataOnlyFlush(t *testing.T) {
	require := require.New(t)
	s, key, _ := newStateBothComplete(t)

	md := metadata.NewTorrentMeta(core.MetaInfoFixture())
	require.NoError(s.SetMetadata(key, md))

	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		var readMd metadata.TorrentMeta
		ok, err := s.impl.disk.GetMetadata(key, &readMd)
		return err == nil && ok
	})
	require.NoError(err)

	var readMd metadata.TorrentMeta
	ok, err := s.impl.disk.GetMetadata(key, &readMd)
	require.NoError(err)
	require.True(ok)
	require.Equal(md.MetaInfo, readMd.MetaInfo)
}

func TestFlusher_HandleFlushFailure(t *testing.T) {
	require := require.New(t)
	s, _ := newTestStore(t, 1*memsize.B, 10*memsize.KB, 1)

	key, _ := createAndWrite(t, s, 1*memsize.KB)
	require.NoError(s.MarkComplete(key))

	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		s.impl.flusher.mu.Lock()
		defer s.impl.flusher.mu.Unlock()
		_, tracked := s.impl.flusher.blobs[key]
		return !tracked
	})
	require.NoError(err)

	_, inDisk := s.impl.disk.Has(key)
	require.False(inDisk)

	_, ok := s.impl.mem.Has(key)
	require.True(ok)

	// The blob should be evictable again after the failed flush left it
	// unbanned: filling the rest of mem's capacity evicts it.
	_, _ = createAndWrite(t, s, 10*memsize.KB)
	_, ok = s.impl.mem.Has(key)
	require.False(ok, "blob should be evictable again after a failed flush left it unbanned")
}

func TestFlusher_DeleteWhileFlushing(t *testing.T) {
	require := require.New(t)
	s, _ := newTestStore(t, 10*memsize.KB, 10*memsize.KB, 1)
	key, _ := createAndWrite(t, s, 1*memsize.KB)

	reached := make(chan struct{})
	proceed := make(chan struct{})
	old := ioCopy
	ioCopy = func(dst io.Writer, src io.Reader) (int64, error) {
		close(reached)
		<-proceed
		return old(dst, src)
	}
	t.Cleanup(func() {
		ioCopy = old
		select {
		case <-proceed:
		default:
			close(proceed)
		}
	})

	require.NoError(s.MarkComplete(key))

	timedOut := false
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		timedOut = true
	}
	require.False(timedOut, "timed out waiting for flushData to reach the copy")

	// disk.Create has already succeeded at this point; the entry exists as an
	// incomplete disk blob. Deleting now races the in-flight flush.
	require.NoError(s.Delete(key))
	close(proceed)

	err := testutil.PollUntilTrue(5*time.Second, func() bool {
		s.impl.flusher.mu.Lock()
		defer s.impl.flusher.mu.Unlock()
		_, tracked := s.impl.flusher.blobs[key]
		return !tracked
	})
	require.NoError(err)

	_, inDisk := s.impl.disk.Has(key)
	require.False(inDisk, "the disk entry created mid-flush must not survive a concurrent Delete")
	_, inMem := s.impl.mem.Has(key)
	require.False(inMem)
}
