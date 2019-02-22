// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package agentserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/scheduler"
	"github.com/uber/kraken/lib/torrent/scheduler/connstate"
	"github.com/uber/kraken/utils/httputil"

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

func TestDownloadNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	namespace := core.TagFixture()
	blob := core.NewBlobFixture()

	mocks.sched.EXPECT().Download(namespace, blob.Digest).Return(scheduler.ErrTorrentNotFound)

	addr := mocks.startServer()
	c := NewClient(addr)

	_, err := c.Download(namespace, blob.Digest)
	require.Error(err)
	require.True(httputil.IsNotFound(err))
}

func TestDownloadUnknownError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	namespace := core.TagFixture()
	blob := core.NewBlobFixture()

	mocks.sched.EXPECT().Download(namespace, blob.Digest).Return(fmt.Errorf("test error"))

	addr := mocks.startServer()
	c := NewClient(addr)

	_, err := c.Download(namespace, blob.Digest)
	require.Error(err)
	require.True(httputil.IsStatus(err, 500))
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
