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
package blobrefresh

import (
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/metainfogen"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/lib/store/tiered"
	mockbackend "github.com/uber/kraken/mocks/lib/backend"
	"github.com/uber/kraken/utils/mockutil"
	"github.com/uber/kraken/utils/testutil"
)

const _testPieceLength = 10

type refresherMocks struct {
	ctrl     *gomock.Controller
	store    *tiered.Store
	backends *backend.Manager
	config   Config
	t        *testing.T
}

func newRefresherMocks(t *testing.T) (*refresherMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	tieredStore, _ := tiered.StoreFixture(t)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	backends := backend.ManagerFixture()

	return &refresherMocks{ctrl, tieredStore, backends, Config{}, t}, cleanup.Run
}

func (m *refresherMocks) new() *Refresher {
	return New(m.config, tally.NoopScope, m.store, m.backends, metainfogen.Fixture(m.store, _testPieceLength))
}

func (m *refresherMocks) newClient(namespace string) *mockbackend.MockClient {
	client := mockbackend.NewMockClient(m.ctrl)
	err := m.backends.Register(namespace, client, false)
	require.NoError(m.t, err)
	return client
}

func TestRefresh(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newRefresherMocks(t)
	defer cleanup()

	refresher := mocks.new()

	namespace := core.TagFixture()
	client := mocks.newClient(namespace)

	blob := core.SizedBlobFixture(100, uint64(_testPieceLength))

	client.EXPECT().Stat(namespace, blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil)
	client.EXPECT().Download(namespace, blob.Digest.Hex(), mockutil.MatchWriter(blob.Content)).Return(nil)

	require.NoError(refresher.Refresh(namespace, blob.Digest))

	// Refresh() returns once the download is enqueued, before the background
	// goroutine driving it (Create -> Download -> MarkComplete -> Generate)
	// finishes. Metainfo generation is that goroutine's last step, so polling
	// for it also guarantees the blob data itself is complete by then.
	var tm metadata.TorrentMeta
	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		ok, err := mocks.store.GetMetadata(blob.Digest.Hex(), &tm)
		return err == nil && ok
	}))
	require.Equal(blob.MetaInfo, tm.MetaInfo)

	f, err := mocks.store.Open(blob.Digest.Hex())
	require.NoError(err)
	result, err := io.ReadAll(f)
	require.NoError(err)
	require.Equal(string(blob.Content), string(result))
}

func TestRefreshSizeLimitError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newRefresherMocks(t)
	defer cleanup()

	mocks.config.SizeLimit = 99

	refresher := mocks.new()

	namespace := core.TagFixture()
	client := mocks.newClient(namespace)

	blob := core.SizedBlobFixture(100, uint64(_testPieceLength))

	client.EXPECT().Stat(namespace, blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil)

	require.Error(refresher.Refresh(namespace, blob.Digest))
}

func TestRefreshSizeLimitWithValidSize(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newRefresherMocks(t)
	defer cleanup()

	mocks.config.SizeLimit = 100

	refresher := mocks.new()

	namespace := core.TagFixture()
	client := mocks.newClient(namespace)

	blob := core.SizedBlobFixture(100, uint64(_testPieceLength))

	client.EXPECT().Stat(namespace, blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil)
	client.EXPECT().Download(namespace, blob.Digest.Hex(), mockutil.MatchWriter(blob.Content)).Return(nil)

	require.NoError(refresher.Refresh(namespace, blob.Digest))

	var tm metadata.TorrentMeta
	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		ok, err := mocks.store.GetMetadata(blob.Digest.Hex(), &tm)
		return err == nil && ok
	}))
}

func TestDedupSameBlobWithDifferentNamespaces(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newRefresherMocks(t)
	defer cleanup()

	refresher := mocks.new()

	namespace1 := core.TagFixture()
	namespace2 := core.TagFixture()
	client1 := mocks.newClient(namespace1)
	client2 := mocks.newClient(namespace2)

	blob := core.SizedBlobFixture(100, uint64(_testPieceLength))

	client1.EXPECT().Stat(namespace1, blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil)
	client2.EXPECT().Stat(namespace2, blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil)

	downloadStarted := make(chan struct{})
	finishDownload := make(chan struct{})

	client1.EXPECT().Download(namespace1, blob.Digest.Hex(), gomock.Any()).DoAndReturn(
		func(_ string, _ string, w io.Writer) error {
			close(downloadStarted)
			<-finishDownload
			_, err := w.Write(blob.Content)
			return err
		},
	)

	require.NoError(refresher.Refresh(namespace1, blob.Digest))
	<-downloadStarted

	err := refresher.Refresh(namespace2, blob.Digest)
	require.Equal(ErrPending, err)

	close(finishDownload)

	var tm metadata.TorrentMeta
	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		ok, err := mocks.store.GetMetadata(blob.Digest.Hex(), &tm)
		return err == nil && ok
	}))
}
