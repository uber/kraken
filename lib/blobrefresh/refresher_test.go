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
package blobrefresh

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/metainfogen"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/mocks/lib/backend"
	"github.com/uber/kraken/utils/mockutil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const _testPieceLength = 10

type refresherMocks struct {
	ctrl     *gomock.Controller
	cas      *store.CAStore
	backends *backend.Manager
	config   Config
}

func newRefresherMocks(t *testing.T) (*refresherMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	cas, c := store.CAStoreFixture()
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	backends := backend.ManagerFixture()

	return &refresherMocks{ctrl, cas, backends, Config{}}, cleanup.Run
}

func (m *refresherMocks) new() *Refresher {
	return New(m.config, tally.NoopScope, m.cas, m.backends, metainfogen.Fixture(m.cas, _testPieceLength))
}

func (m *refresherMocks) newClient(namespace string) *mockbackend.MockClient {
	client := mockbackend.NewMockClient(m.ctrl)
	m.backends.Register(namespace, client)
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

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := mocks.cas.GetCacheFileStat(blob.Digest.Hex())
		return !os.IsNotExist(err)
	}))

	f, err := mocks.cas.GetCacheFileReader(blob.Digest.Hex())
	require.NoError(err)
	result, err := ioutil.ReadAll(f)
	require.Equal(string(blob.Content), string(result))

	var tm metadata.TorrentMeta
	require.NoError(mocks.cas.GetCacheFileMetadata(blob.Digest.Hex(), &tm))
	require.Equal(blob.MetaInfo, tm.MetaInfo)
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

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := mocks.cas.GetCacheFileStat(blob.Digest.Hex())
		return !os.IsNotExist(err)
	}))
}
