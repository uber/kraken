package agentserver

import (
	"fmt"
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"github.com/stretchr/testify/require"
)

func TestGetBlobHandler(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	d := image.DigestFixture()

	mocks.torrentClient.EXPECT().Download(d.Hex()).Return(nil, nil)

	addr := mocks.startServer()

	_, err := http.Get(fmt.Sprintf("http://%s/blobs/%s", addr, d.Hex()))
	require.NoError(err)
}

func TestHealthHandler(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	_, err := http.Get(fmt.Sprintf("http://%s/health", addr))
	require.NoError(err)
}
