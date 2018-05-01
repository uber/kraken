package remotes

import (
	"testing"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/mocks/build-index/tagclient"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type replicatorMocks struct {
	ctrl          *gomock.Controller
	originCluster *mockblobclient.MockClusterClient
	provider      *tagclient.TestProvider
}

func newReplicatorMocks(t *testing.T) (*replicatorMocks, func()) {
	ctrl := gomock.NewController(t)
	return &replicatorMocks{
		ctrl,
		mockblobclient.NewMockClusterClient(ctrl),
		tagclient.NewTestProvider(),
	}, ctrl.Finish
}

func (m *replicatorMocks) new(config Config) (Replicator, error) {
	return New(config, m.originCluster, m.provider)
}

func TestReplicatorReplicate(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReplicatorMocks(t)
	defer cleanup()

	sjc1 := mocktagclient.NewMockClient(mocks.ctrl)
	dca1 := mocktagclient.NewMockClient(mocks.ctrl)
	wbu2 := mocktagclient.NewMockClient(mocks.ctrl)

	mocks.provider.Register("build-index-sjc1", sjc1)
	mocks.provider.Register("build-index-dca1", dca1)
	mocks.provider.Register("build-index-wbu2", wbu2)

	config := Config{
		"prime/.*": []string{
			"build-index-sjc1",
			"build-index-dca1",
		},
		"all/.*": []string{
			"build-index-sjc1",
			"build-index-dca1",
			"build-index-wbu2",
		},
	}

	replicator, err := mocks.new(config)
	require.NoError(err)

	tag := "prime/labrat"
	digest := core.DigestFixture()
	deps := []core.Digest{
		core.DigestFixture(), core.DigestFixture(), core.DigestFixture()}

	gomock.InOrder(
		sjc1.EXPECT().Origin().Return("origin-sjc1", nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, deps[0], "origin-sjc1").Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, deps[1], "origin-sjc1").Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, deps[2], "origin-sjc1").Return(nil),
		sjc1.EXPECT().Put(tag, digest).Return(nil),
	)
	gomock.InOrder(
		dca1.EXPECT().Origin().Return("origin-dca1", nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, deps[0], "origin-dca1").Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, deps[1], "origin-dca1").Return(nil),
		mocks.originCluster.EXPECT().ReplicateToRemote(tag, deps[2], "origin-dca1").Return(nil),
		dca1.EXPECT().Put(tag, digest).Return(nil),
	)
	// WBU2 is skipped since it does not match the tag.

	require.NoError(replicator.Replicate(tag, digest, deps))
}
