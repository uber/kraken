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
package tagreplication

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	mocktagclient "github.com/uber/kraken/mocks/build-index/tagclient"
	mockblobclient "github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/mockutil"
)

const (
	_testRemoteOrigin = "some-remote-origin"
)

type executorMocks struct {
	ctrl              *gomock.Controller
	originCluster     *mockblobclient.MockClusterClient
	tagClientProvider *mocktagclient.MockProvider
}

func newExecutorMocks(t *testing.T) (*executorMocks, func()) {
	ctrl := gomock.NewController(t)
	return &executorMocks{
		ctrl:              ctrl,
		originCluster:     mockblobclient.NewMockClusterClient(ctrl),
		tagClientProvider: mocktagclient.NewMockProvider(ctrl),
	}, ctrl.Finish
}

func (m *executorMocks) new() *Executor {
	return NewExecutor(tally.NoopScope, m.originCluster, m.tagClientProvider)
}

func (m *executorMocks) newTagClient() *mocktagclient.MockClient {
	return mocktagclient.NewMockClient(m.ctrl)
}

func TestExecutor(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	executor := mocks.new()
	tagClient := mocks.newTagClient()

	tag := core.TagFixture()
	layers := core.DigestListFixture(3)
	manifest, manifestBytes := dockerutil.ManifestFixture(layers[0], layers[1], layers[2])
	task := NewTask(tag, manifest, core.DigestListFixture(3), "some-dest", 0)

	gomock.InOrder(
		mocks.tagClientProvider.EXPECT().Provide(task.Destination).Return(tagClient),
		tagClient.EXPECT().Has(task.Tag).Return(false, nil),
		tagClient.EXPECT().Origin().Return(_testRemoteOrigin, nil),
		mocks.originCluster.EXPECT().
			DownloadBlob(gomock.Any(), tag, manifest, mockutil.MatchWriter(manifestBytes)).
			Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, manifest, _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, layers[0], _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, layers[1], _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, layers[2], _testRemoteOrigin).Return(nil),
		tagClient.EXPECT().PutAndReplicate(task.Tag, task.Digest).Return(nil),
	)

	require.NoError(executor.Exec(task))
}

func TestExecutorNoopsWhenTagAlreadyReplicated(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	executor := mocks.new()
	tagClient := mocks.newTagClient()
	task := TaskFixture()

	gomock.InOrder(
		mocks.tagClientProvider.EXPECT().Provide(task.Destination).Return(tagClient),
		tagClient.EXPECT().Has(task.Tag).Return(true, nil),
	)

	require.NoError(executor.Exec(task))
}

func TestExecutorManifestList(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	executor := mocks.new()
	tagClient := mocks.newTagClient()

	tag := core.TagFixture()
	m1Layers := core.DigestListFixture(3)
	m2Layers := core.DigestListFixture(3)
	m1Digest, m1Bytes := dockerutil.ManifestFixture(m1Layers[0], m1Layers[1], m1Layers[2])
	m2Digest, m2Bytes := dockerutil.ManifestFixture(m2Layers[0], m2Layers[1], m2Layers[2])
	mlDigest, mlBytes := dockerutil.ManifestListFixture(m1Digest, m2Digest)

	task := NewTask(tag, mlDigest, core.DigestList{m1Digest, m2Digest}, "some-dest", 0)

	gomock.InOrder(
		mocks.tagClientProvider.EXPECT().Provide(task.Destination).Return(tagClient),
		tagClient.EXPECT().Has(task.Tag).Return(false, nil),
		tagClient.EXPECT().Origin().Return(_testRemoteOrigin, nil),
		mocks.originCluster.EXPECT().
			DownloadBlob(gomock.Any(), tag, mlDigest, mockutil.MatchWriter(mlBytes)).
			Return(nil),
		mocks.originCluster.EXPECT().
			DownloadBlob(gomock.Any(), tag, m1Digest, mockutil.MatchWriter(m1Bytes)).
			Return(nil),
		mocks.originCluster.EXPECT().
			DownloadBlob(gomock.Any(), tag, m2Digest, mockutil.MatchWriter(m2Bytes)).
			Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, mlDigest, _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, m1Digest, _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, m1Layers[0], _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, m1Layers[1], _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, m1Layers[2], _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, m2Digest, _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, m2Layers[0], _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, m2Layers[1], _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, m2Layers[2], _testRemoteOrigin).Return(nil),
		tagClient.EXPECT().PutAndReplicate(task.Tag, task.Digest).Return(nil),
	)

	require.NoError(executor.Exec(task))
}

func TestExecutorSubManifestDownloadError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	executor := mocks.new()
	tagClient := mocks.newTagClient()

	tag := core.TagFixture()
	m1Digest, _ := dockerutil.ManifestFixture(core.DigestFixture(), core.DigestFixture(), core.DigestFixture())
	mlDigest, mlBytes := dockerutil.ManifestListFixture(m1Digest, core.DigestFixture())

	task := NewTask(tag, mlDigest, core.DigestList{m1Digest}, "some-dest", 0)

	gomock.InOrder(
		mocks.tagClientProvider.EXPECT().Provide(task.Destination).Return(tagClient),
		tagClient.EXPECT().Has(task.Tag).Return(false, nil),
		tagClient.EXPECT().Origin().Return(_testRemoteOrigin, nil),
		mocks.originCluster.EXPECT().
			DownloadBlob(gomock.Any(), tag, mlDigest, mockutil.MatchWriter(mlBytes)).
			Return(nil),
		mocks.originCluster.EXPECT().
			DownloadBlob(gomock.Any(), tag, m1Digest, gomock.Any()).
			Return(blobclient.ErrBlobNotFound),
	)

	err := executor.Exec(task)
	require.ErrorContains(err, "download sub-manifest")
}
