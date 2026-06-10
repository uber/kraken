// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package transfer

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/scheduler"
	mocktagclient "github.com/uber/kraken/mocks/build-index/tagclient"
	mockscheduler "github.com/uber/kraken/mocks/lib/torrent/scheduler"
	"github.com/uber/kraken/utils/memsize"
	"github.com/uber/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

// fakeBlobReader adapts a bytes.Reader to scheduler.BlobReader for mocking the
// streaming download path. bytes.Reader already provides Read, ReadAt, Seek and
// Size; only Close is added.
type fakeBlobReader struct {
	*bytes.Reader
}

func (fakeBlobReader) Close() error { return nil }

func newFakeBlobReader(b []byte) scheduler.BlobReader {
	return fakeBlobReader{bytes.NewReader(b)}
}

type agentTransfererMocks struct {
	cads  *store.CADownloadStore
	tags  *mocktagclient.MockClient
	sched *mockscheduler.MockScheduler
	stats tally.TestScope
}

func newReadOnlyTransfererMocks(t *testing.T) (*agentTransfererMocks, func()) {
	var cleanup testutil.Cleanup

	cads, c := store.CADownloadStoreFixture()
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	tags := mocktagclient.NewMockClient(ctrl)

	sched := mockscheduler.NewMockScheduler(ctrl)

	stats := tally.NewTestScope("", nil)

	return &agentTransfererMocks{cads, tags, sched, stats}, cleanup.Run
}

func (m *agentTransfererMocks) new() *ReadOnlyTransferer {
	return NewReadOnlyTransferer(m.stats, m.cads, m.tags, m.sched)
}

// mbServedValue returns the "mb_served" counter value from the scope.
func mbServedValue(scope tally.TestScope) int64 {
	for _, c := range scope.Snapshot().Counters() {
		if c.Name() == "mb_served" {
			return c.Value()
		}
	}
	return 0
}

func TestReadOnlyTransfererDownloadStreamsOnCacheMiss(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/repo-bar:latest"
	blob := core.NewBlobFixture()

	mocks.sched.EXPECT().DownloadReader(
		namespace, blob.Digest).Return(newFakeBlobReader(blob.Content), nil)

	result, err := transferer.Download(namespace, blob.Digest)
	require.NoError(err)
	b, err := io.ReadAll(result)
	require.NoError(err)
	require.Equal(blob.Content, b)
}

func TestReadOnlyTransfererDownloadReadsFromCache(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/repo-bar:latest"
	blob := core.NewBlobFixture()
	require.NoError(store.RunDownload(mocks.cads, blob.Digest, blob.Content))

	// A cached blob is served from cache without invoking the scheduler.
	for i := 0; i < 10; i++ {
		result, err := transferer.Download(namespace, blob.Digest)
		require.NoError(err)
		b, err := io.ReadAll(result)
		require.NoError(err)
		require.Equal(blob.Content, b)
	}
}

func TestReadOnlyTransfererDownloadEmitsMBServed(t *testing.T) {
	for _, tc := range []struct {
		desc     string
		blobSize uint64
		wantMB   int64
	}{
		{"large blob (2 MiB)", 2 * memsize.MB, 2},
		{"exact 1 MiB blob", memsize.MB, 1},
		{"sub-MiB blob truncates to 0", 256 * memsize.KB, 0},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require := require.New(t)

			mocks, cleanup := newReadOnlyTransfererMocks(t)
			defer cleanup()

			transferer := mocks.new()

			namespace := "docker/repo-bar:latest"
			blob := core.SizedBlobFixture(tc.blobSize, 64)

			mocks.sched.EXPECT().DownloadReader(
				namespace, blob.Digest).Return(newFakeBlobReader(blob.Content), nil)

			result, err := transferer.Download(namespace, blob.Digest)
			require.NoError(err)
			_, err = io.ReadAll(result)
			require.NoError(err)

			require.Equal(tc.wantMB, mbServedValue(mocks.stats))
		})
	}
}

// TestReadOnlyTransfererDownloadEmitsMBServedOnCacheHit documents that the
// mb_served counter increments on every Download call, including cached reads
// where no blob was actually fetched over the network.
func TestReadOnlyTransfererDownloadEmitsMBServedOnCacheHit(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/repo-bar:latest"
	blob := core.SizedBlobFixture(2*memsize.MB, 64)
	require.NoError(store.RunDownload(mocks.cads, blob.Digest, blob.Content))

	for i := 0; i < 3; i++ {
		result, err := transferer.Download(namespace, blob.Digest)
		require.NoError(err)
		_, err = io.ReadAll(result)
		require.NoError(err)
	}

	require.Equal(int64(6), mbServedValue(mocks.stats))
}

func TestReadOnlyTransfererStatStreamsOnCacheMiss(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/repo-bar:latest"
	blob := core.NewBlobFixture()

	mocks.sched.EXPECT().DownloadReader(
		namespace, blob.Digest).Return(newFakeBlobReader(blob.Content), nil)

	bi, err := transferer.Stat(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(blob.Info(), bi)
}

func TestReadOnlyTransfererStatReadsFromCache(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/repo-bar:latest"
	blob := core.NewBlobFixture()
	require.NoError(store.RunDownload(mocks.cads, blob.Digest, blob.Content))

	// A cached blob is stat-ed from cache without invoking the scheduler.
	for i := 0; i < 10; i++ {
		bi, err := transferer.Stat(namespace, blob.Digest)
		require.NoError(err)
		require.Equal(blob.Info(), bi)
	}
}

func TestReadOnlyTransfererGetTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	tag := "docker/some-tag"
	manifest := core.DigestFixture()

	mocks.tags.EXPECT().Get(tag).Return(manifest, nil)

	d, err := transferer.GetTag(tag)
	require.NoError(err)
	require.Equal(manifest, d)
}

func TestReadOnlyTransfererGetTagNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	tag := "docker/some-tag"

	mocks.tags.EXPECT().Get(tag).Return(core.Digest{}, tagclient.ErrTagNotFound)

	_, err := transferer.GetTag(tag)
	require.Error(err)
	require.Equal(ErrTagNotFound, err)
}

func TestReadOnlyTransfererMultipleDownloadsOfSameBlob(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/repo-bar:latest"
	blob := core.NewBlobFixture()

	mocks.sched.EXPECT().DownloadReader(namespace, blob.Digest).DoAndReturn(
		func(namespace string, d core.Digest) (scheduler.BlobReader, error) {
			return newFakeBlobReader(blob.Content), nil
		}).Times(10)

	// Multiple clients streaming the same uncached blob each get an independent
	// reader.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := transferer.Download(namespace, blob.Digest)
			require.NoError(err)
			b, err := io.ReadAll(result)
			require.NoError(err)
			require.Equal(blob.Content, b)
		}()
	}

	wg.Wait()
}
