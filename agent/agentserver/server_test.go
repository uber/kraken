package agentserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/connstate"
	"code.uber.internal/infra/kraken/utils/httputil"
	"github.com/stretchr/testify/require"
)

func TestDownload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	namespace := core.TagFixture()
	blob := core.NewBlobFixture()

	mocks.sched.EXPECT().Download(namespace, blob.Digest).DoAndReturn(
		func(namespace string, d core.Digest) error {
			return store.RunDownload(mocks.cads, d, blob.Content)
		})

	addr := mocks.startServer()
	c := NewClient(addr)

	r, err := c.Download(namespace, blob.Digest)
	require.NoError(err)
	result, err := ioutil.ReadAll(r)
	require.NoError(err)
	require.Equal(string(blob.Content), string(result))
}

func TestHealthHandler(t *testing.T) {
	tests := []struct {
		desc     string
		probeErr error
	}{
		{"probe error", errors.New("some probe error")},
		{"healthy", nil},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			mocks, cleanup := newServerMocks(t)
			defer cleanup()

			mocks.sched.EXPECT().Probe().Return(test.probeErr)

			addr := mocks.startServer()

			_, err := httputil.Get(fmt.Sprintf("http://%s/health", addr))
			if test.probeErr != nil {
				require.Error(err)
			} else {
				require.NoError(err)
			}
		})
	}
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

	mocks.sched.EXPECT().Reload(config)

	_, err = httputil.Patch(
		fmt.Sprintf("http://%s/x/config/scheduler", addr),
		httputil.SendBody(bytes.NewReader(b)))
	require.NoError(err)
}

func TestGetBlacklistHandler(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	blacklist := []connstate.BlacklistedConn{{
		PeerID:    core.PeerIDFixture(),
		InfoHash:  core.InfoHashFixture(),
		Remaining: time.Second,
	}}
	mocks.sched.EXPECT().BlacklistSnapshot().Return(blacklist, nil)

	addr := mocks.startServer()

	resp, err := httputil.Get(fmt.Sprintf("http://%s/x/blacklist", addr))
	require.NoError(err)

	var result []connstate.BlacklistedConn
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

	mocks.sched.EXPECT().RemoveTorrent(d).Return(nil)

	require.NoError(c.Delete(d))
}
