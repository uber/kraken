// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package originstorage

import (
	"os"
	"testing"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/blobrefresh"
	"github.com/uber/kraken/lib/metainfogen"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/mocks/lib/backend"
	"github.com/uber/kraken/utils/mockutil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const pieceLength = 4

type archiveMocks struct {
	cas           *store.CAStore
	backendClient *mockbackend.MockClient
	blobRefresher *blobrefresh.Refresher
}

func newArchiveMocks(t *testing.T, namespace string) (*archiveMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	cas, c := store.CAStoreFixture()
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	backendClient := mockbackend.NewMockClient(ctrl)
	backends := backend.ManagerFixture()
	backends.Register(namespace, backendClient)

	blobRefresher := blobrefresh.New(
		blobrefresh.Config{}, tally.NoopScope, cas, backends, metainfogen.Fixture(cas, pieceLength))

	return &archiveMocks{cas, backendClient, blobRefresher}, cleanup.Run
}

func (m *archiveMocks) new() *TorrentArchive {
	return NewTorrentArchive(m.cas, m.blobRefresher)
}

func TestTorrentArchiveStatNoExistTriggersRefresh(t *testing.T) {
	require := require.New(t)

	namespace := core.TagFixture()
	mocks, cleanup := newArchiveMocks(t, namespace)
	defer cleanup()

	archive := mocks.new()

	blob := core.SizedBlobFixture(100, pieceLength)

	mocks.backendClient.EXPECT().Stat(namespace,
		blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil)
	mocks.backendClient.EXPECT().Download(namespace, blob.Digest.Hex(), mockutil.MatchWriter(blob.Content))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := archive.Stat(namespace, blob.Digest)
		return err == nil
	}))

	info, err := archive.Stat(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(blob.Digest, info.Digest())
	require.Equal(blob.MetaInfo.InfoHash(), info.InfoHash())
	require.Equal(100, info.PercentDownloaded())
}

func TestTorrentArchiveGetTorrentNoExistTriggersRefresh(t *testing.T) {
	require := require.New(t)

	namespace := core.TagFixture()
	mocks, cleanup := newArchiveMocks(t, namespace)
	defer cleanup()

	archive := mocks.new()

	blob := core.SizedBlobFixture(100, pieceLength)

	mocks.backendClient.EXPECT().Stat(namespace,
		blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil)
	mocks.backendClient.EXPECT().Download(namespace, blob.Digest.Hex(), mockutil.MatchWriter(blob.Content))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := archive.GetTorrent(namespace, blob.Digest)
		return err == nil
	}))

	tor, err := archive.GetTorrent(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(blob.Digest, tor.Digest())
	require.Equal(blob.MetaInfo.InfoHash(), tor.InfoHash())
	require.True(tor.Complete())
}

func TestTorrentArchiveDeleteTorrent(t *testing.T) {
	require := require.New(t)

	namespace := core.TagFixture()
	mocks, cleanup := newArchiveMocks(t, namespace)
	defer cleanup()

	archive := mocks.new()

	blob := core.SizedBlobFixture(100, pieceLength)

	mocks.backendClient.EXPECT().Stat(namespace,
		blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil)
	mocks.backendClient.EXPECT().Download(namespace, blob.Digest.Hex(), mockutil.MatchWriter(blob.Content))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := archive.Stat(namespace, blob.Digest)
		return err == nil
	}))

	require.NoError(archive.DeleteTorrent(blob.Digest))

	_, err := mocks.cas.GetCacheFileStat(blob.Digest.Hex())
	require.True(os.IsNotExist(err))
}
