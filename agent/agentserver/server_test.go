package agentserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/utils/httputil"
	"github.com/stretchr/testify/require"
)

const namespace = "test-namespace"

func TestDownload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	blob := core.NewBlobFixture()

	mocks.torrentClient.EXPECT().Download(namespace, blob.Digest.Hex()).DoAndReturn(
		func(namespace, name string) error {
			return mocks.fs.CreateCacheFile(name, bytes.NewReader(blob.Content))
		})

	addr := mocks.startServer()
	c := NewClient(addr)

	r, err := c.Download(namespace, blob.Digest.Hex())
	require.NoError(err)
	result, err := ioutil.ReadAll(r)
	require.NoError(err)
	require.Equal(string(blob.Content), string(result))
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
		ConnTTI: time.Minute,
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
		PeerID:    core.PeerIDFixture(),
		InfoHash:  core.InfoHashFixture(),
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

func TestDeleteBlobHandler(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	d := core.DigestFixture()

	addr := mocks.startServer()
	c := NewClient(addr)

	mocks.torrentClient.EXPECT().RemoveTorrent(d.Hex()).Return(nil)

	require.NoError(c.Delete(d.Hex()))
}
