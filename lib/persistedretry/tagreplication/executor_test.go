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
package tagreplication

import (
	"testing"

	"github.com/uber/kraken/mocks/build-index/tagclient"
	"github.com/uber/kraken/mocks/origin/blobclient"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
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
	task := TaskFixture()

	gomock.InOrder(
		mocks.tagClientProvider.EXPECT().Provide(task.Destination).Return(tagClient),
		tagClient.EXPECT().Has(task.Tag).Return(false, nil),
		tagClient.EXPECT().Origin().Return(_testRemoteOrigin, nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(
			task.Tag, task.Dependencies[0], _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(
			task.Tag, task.Dependencies[1], _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(
			task.Tag, task.Dependencies[2], _testRemoteOrigin).Return(nil),
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
