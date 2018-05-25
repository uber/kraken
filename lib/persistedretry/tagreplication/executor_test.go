package tagreplication

import (
	"testing"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/mocks/build-index/tagclient"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"

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
		tagClient.EXPECT().Get(task.Tag).Return(core.Digest{}, tagclient.ErrNotFound),
		tagClient.EXPECT().Origin().Return(_testRemoteOrigin, nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(
			task.Tag, task.Dependencies[0], _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(
			task.Tag, task.Dependencies[1], _testRemoteOrigin).Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(
			task.Tag, task.Dependencies[2], _testRemoteOrigin).Return(nil),
		tagClient.EXPECT().Put(task.Tag, task.Digest).Return(nil),
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
		tagClient.EXPECT().Get(task.Tag).Return(task.Digest, nil),
	)

	require.NoError(executor.Exec(task))
}
