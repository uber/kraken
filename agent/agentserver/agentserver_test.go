package agentserver

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"github.com/stretchr/testify/require"
)

func TestGetBlobHandler(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	d, blob := image.DigestWithBlobFixture()

	mocks.torrentClient.EXPECT().DownloadTorrent(d.Hex()).
		Return(ioutil.NopCloser(bytes.NewBuffer(blob)), nil)

	addr := mocks.startServer()

	resp, err := http.Get(fmt.Sprintf("http://%s/blobs/%s", addr, d.Hex()))
	require.NoError(err)
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(err)

	filepath := string(body)
	result, err := ioutil.ReadFile(filepath)
	require.NoError(err)
	require.Equal(blob, result)
}

func TestHealthHandler(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	_, err := http.Get(fmt.Sprintf("http://%s/health", addr))
	require.NoError(err)
}
