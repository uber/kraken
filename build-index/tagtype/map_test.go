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

func testConfigs() []Config {
	return []Config{
		{Namespace: "uber-usi/.*", Type: "docker"},
		{Namespace: "replicator/.*", Type: "default"},
	}
}

func TestMapResolveDocker(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originClient := mockblobclient.NewMockClusterClient(ctrl)

	m, err := NewMap(testConfigs(), originClient)
	require.NoError(err)

	tag := "uber-usi/labrat:0001"
	layers := core.DigestListFixture(3)
	manifest, b := dockerutil.ManifestFixture(layers[0], layers[1], layers[2])

	originClient.EXPECT().DownloadBlob(tag, manifest, mockutil.MatchWriter(b)).Return(nil)

	deps, err := m.Resolve(tag, manifest)
	require.NoError(err)
	require.Equal(core.DigestList(append(layers, manifest)), deps)
}

func TestMapResolveDefault(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originClient := mockblobclient.NewMockClusterClient(ctrl)

	m, err := NewMap(testConfigs(), originClient)
	require.NoError(err)

	tag := "replicator/labrat:0001"
	d := core.DigestFixture()

	deps, err := m.Resolve(tag, d)
	require.NoError(err)
	require.Equal(core.DigestList{d}, deps)
}

func TestMapGetResolverNamespaceError(t *testing.T) {
	require := require.New(t)

	m, err := NewMap(testConfigs(), nil)
	require.NoError(err)

	_, err = m.Resolve("invalid/tag", core.DigestFixture())
	require.Error(err)
	require.Equal(errNamespaceNotFound, err)
}
