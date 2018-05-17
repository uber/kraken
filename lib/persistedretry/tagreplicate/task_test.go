package tagreplicate

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/mocks/build-index/tagclient"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
)

func TestRun(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tagProvider := tagclient.NewTestProvider()
	dca1 := mocktagclient.NewMockClient(ctrl)
	tagProvider.Register("build-index-dca1", dca1)
	localOriginClient := mockblobclient.NewMockClusterClient(ctrl)

	stats := tally.NoopScope
	tag := "prime/labrat"
	digest := core.DigestFixture()
	deps := []core.Digest{
		core.DigestFixture(), core.DigestFixture(), core.DigestFixture()}

	task := NewTask(localOriginClient, tagProvider, stats, tag, "build-index-dca1", digest, deps...)

	gomock.InOrder(
		dca1.EXPECT().Origin().Return("origin-dca1", nil),
		localOriginClient.EXPECT().ReplicateToRemote(tag, deps[0], "origin-dca1").Return(nil),
		localOriginClient.EXPECT().ReplicateToRemote(tag, deps[1], "origin-dca1").Return(nil),
		localOriginClient.EXPECT().ReplicateToRemote(tag, deps[2], "origin-dca1").Return(nil),
		dca1.EXPECT().Put(tag, digest).Return(nil),
	)

	require.NoError(task.Run())
}
