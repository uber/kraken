package tagtype

import (
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/dockerutil"
	"code.uber.internal/infra/kraken/utils/mockutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestDockerGetDependencies(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originClient := mockblobclient.NewMockClusterClient(ctrl)
	r := NewDockerResolver(originClient)

	tag := core.TagFixture()
	layers := core.DigestListFixture(3)
	manifest, b := dockerutil.ManifestFixture(layers[0], layers[1], layers[2])

	originClient.EXPECT().DownloadBlob(tag, manifest, mockutil.MatchWriter(b)).Return(nil)
	deps, err := r.Resolve(tag, manifest)
	require.NoError(err)
	require.Equal(core.DigestList(append(layers, manifest)), deps)
}
