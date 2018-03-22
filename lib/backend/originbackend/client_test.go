package originbackend

import (
	"bytes"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/rwutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type clientMocks struct {
	cluster *mockblobclient.MockClusterClient
}

func newClientMocks(t *testing.T) (*clientMocks, func()) {
	ctrl := gomock.NewController(t)
	return &clientMocks{
		cluster: mockblobclient.NewMockClusterClient(ctrl),
	}, ctrl.Finish
}

func (m *clientMocks) newClient(config Config) *Client {
	return newClient(config, m.cluster)
}

func TestNewClient(t *testing.T) {
	client, err := NewClient(Config{
		Namespace:  "test-namespace",
		RoundRobin: serverset.RoundRobinConfig{Addrs: []string{"o1", "o2"}},
	})
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestClientUpload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.newClient(Config{Namespace: "test-namespace"})

	blob := core.NewBlobFixture()

	mocks.cluster.EXPECT().UploadBlob(
		"test-namespace", blob.Digest, rwutil.MatchReader(blob.Content), true).Return(nil)

	require.NoError(client.Upload(blob.Digest.Hex(), bytes.NewReader(blob.Content)))
}
