package agentserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/httputil"
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

func TestPatchSchedulerConfigHandler(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	config := scheduler.Config{
		IdleConnTTL: time.Minute,
	}
	b, err := json.Marshal(config)
	require.NoError(err)

	mocks.torrentClient.EXPECT().Reload(config)

	_, err = httputil.Patch(
		fmt.Sprintf("http://%s/x/config/scheduler", addr),
		httputil.SendBody(bytes.NewReader(b)))
	require.NoError(err)
}

func TestGetBlacklistHandler(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	blacklist := []scheduler.BlacklistedConn{{
		PeerID:    torlib.PeerIDFixture(),
		InfoHash:  torlib.InfoHashFixture(),
		Remaining: time.Second,
	}}
	mocks.torrentClient.EXPECT().BlacklistSnapshot().Return(blacklist, nil)

	addr := mocks.startServer()

	resp, err := httputil.Get(fmt.Sprintf("http://%s/x/blacklist", addr))
	require.NoError(err)

	var result []scheduler.BlacklistedConn
	require.NoError(json.NewDecoder(resp.Body).Decode(&result))
	require.Equal(blacklist, result)
}
