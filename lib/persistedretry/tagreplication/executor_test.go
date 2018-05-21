package tagreplication

import (
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/mocks/build-index/tagclient"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestExecutor(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originCluster := mockblobclient.NewMockClusterClient(ctrl)

	tagClientProvider := mocktagclient.NewMockProvider(ctrl)

	tagClient := mocktagclient.NewMockClient(ctrl)

	executor := NewExecutor(tally.NoopScope, originCluster, tagClientProvider)

	remoteOrigin := "some-remote-origin"

	task := NewTask(
		"labrat:latest",
		core.DigestFixture(),
		core.DigestListFixture(3),
		"some-remote-build-index")

	gomock.InOrder(
		tagClientProvider.EXPECT().Provide(task.Destination).Return(tagClient),
		tagClient.EXPECT().Origin().Return(remoteOrigin, nil),
		originCluster.EXPECT().ReplicateToRemote(
			task.Tag, task.Dependencies[0], remoteOrigin).Return(nil),
		originCluster.EXPECT().ReplicateToRemote(
			task.Tag, task.Dependencies[1], remoteOrigin).Return(nil),
		originCluster.EXPECT().ReplicateToRemote(
			task.Tag, task.Dependencies[2], remoteOrigin).Return(nil),
		tagClient.EXPECT().Put(task.Tag, task.Digest).Return(nil),
	)

	require.NoError(executor.Exec(task))
}
