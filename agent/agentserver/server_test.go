// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package agentserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/uber/kraken/agent/agentclient"
	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/scheduler"
	"github.com/uber/kraken/lib/torrent/scheduler/connstate"
	mocktagclient "github.com/uber/kraken/mocks/build-index/tagclient"
	mockcontainerruntime "github.com/uber/kraken/mocks/lib/containerruntime"
	mockcontainerd "github.com/uber/kraken/mocks/lib/containerruntime/containerd"
	mockdockerdaemon "github.com/uber/kraken/mocks/lib/containerruntime/dockerdaemon"
	mockscheduler "github.com/uber/kraken/mocks/lib/torrent/scheduler"
	mockannounceclient "github.com/uber/kraken/mocks/tracker/announceclient"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

type serverMocks struct {
	cads             *store.CADownloadStore
	sched            *mockscheduler.MockReloadableScheduler
	tags             *mocktagclient.MockClient
	dockerCli        *mockdockerdaemon.MockDockerClient
	containerdCli    *mockcontainerd.MockClient
	ac               *mockannounceclient.MockClient
	containerRuntime *mockcontainerruntime.MockFactory
	cleanup          *testutil.Cleanup
}

func newServerMocks(t *testing.T) (*serverMocks, func()) {
	var cleanup testutil.Cleanup

	cads, c := store.CADownloadStoreFixture()
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	sched := mockscheduler.NewMockReloadableScheduler(ctrl)

	tags := mocktagclient.NewMockClient(ctrl)

	dockerCli := mockdockerdaemon.NewMockDockerClient(ctrl)
	containerdCli := mockcontainerd.NewMockClient(ctrl)
	ac := mockannounceclient.NewMockClient(ctrl)
	containerruntime := mockcontainerruntime.NewMockFactory(ctrl)
	return &serverMocks{
		cads, sched, tags, dockerCli, containerdCli, ac,
		containerruntime, &cleanup}, cleanup.Run
}

func (m *serverMocks) startServer(c Config) (*Server, string) {
	s := New(c, tally.NoopScope, m.cads, m.sched, m.tags, m.ac, m.containerRuntime)
	addr, stop := testutil.StartServer(s.Handler())
	m.cleanup.Add(stop)
	return s, addr
}

func TestGetTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	tag := core.TagFixture()
	d := core.DigestFixture()

	mocks.tags.EXPECT().Get(tag).Return(d, nil)
	_, addr := mocks.startServer(Config{})
	c := agentclient.New(addr)

	result, err := c.GetTag(tag)
	require.NoError(err)
	require.Equal(d, result)
}

func TestGetTagNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	tag := core.TagFixture()

	mocks.tags.EXPECT().Get(tag).Return(core.Digest{}, tagclient.ErrTagNotFound)

	_, addr := mocks.startServer(Config{})
	c := agentclient.New(addr)

	_, err := c.GetTag(tag)
	require.Error(err)
	require.Equal(agentclient.ErrTagNotFound, err)
}

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

	_, addr := mocks.startServer(Config{})
	c := agentclient.New(addr)

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

	_, addr := mocks.startServer(Config{})
	c := agentclient.New(addr)

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

	_, addr := mocks.startServer(Config{})
	c := agentclient.New(addr)

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

			_, addr := mocks.startServer(Config{})

			_, err := httputil.Get(fmt.Sprintf("http://%s/health", addr))
			if test.probeErr != nil {
				require.Error(err)
			} else {
				require.NoError(err)
			}
		})
	}
}

func TestReadinessCheckHandler(t *testing.T) {
	for _, tc := range []struct {
		desc          string
		probeErr      error
		buildIndexErr error
		trackerErr    error
		wantErr       string
	}{
		{
			desc:          "success",
			probeErr:      nil,
			buildIndexErr: nil,
			trackerErr:    nil,
			wantErr:       "",
		},
		{
			desc:          "failure (probe fails)",
			probeErr:      errors.New("test scheduler error"),
			buildIndexErr: nil,
			trackerErr:    nil,
			wantErr:       "GET http://{address}/readiness 503: agent not ready: test scheduler error",
		},
		{
			desc:          "failure (build index not ready)",
			probeErr:      nil,
			buildIndexErr: errors.New("build index not ready"),
			trackerErr:    nil,
			wantErr:       "GET http://{address}/readiness 503: agent not ready: build index not ready",
		},
		{
			desc:          "failure (tracker not ready)",
			probeErr:      nil,
			buildIndexErr: nil,
			trackerErr:    errors.New("tracker not ready"),
			wantErr:       "GET http://{address}/readiness 503: agent not ready: tracker not ready",
		},
		{
			desc:          "failure (all conditions fail)",
			probeErr:      errors.New("test scheduler error"),
			buildIndexErr: errors.New("build index not ready"),
			trackerErr:    errors.New("tracker not ready"),
			wantErr:       "GET http://{address}/readiness 503: agent not ready: test scheduler error\nbuild index not ready\ntracker not ready",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require := require.New(t)

			mocks, cleanup := newServerMocks(t)
			defer cleanup()

			mocks.sched.EXPECT().Probe().Return(tc.probeErr)
			mocks.tags.EXPECT().CheckReadiness().Return(tc.buildIndexErr)
			mocks.ac.EXPECT().CheckReadiness().Return(tc.trackerErr)

			_, addr := mocks.startServer(Config{})
			_, err := httputil.Get(fmt.Sprintf("http://%s/readiness", addr))
			if tc.wantErr == "" {
				require.Nil(err)
			} else {
				require.EqualError(err, strings.ReplaceAll(tc.wantErr, "{address}", addr))
			}
		})
	}
}

func TestReadinessCheckHandlerCache(t *testing.T) {
	testErr := errors.New("test-err")

	for _, tc := range []struct {
		desc                  string
		readinessCacheTTL     time.Duration
		waitInvalidation      bool
		wantFirstCallSuccess  bool
		wantSecondCallSuccess bool
		setupMocks            func(m *serverMocks)
	}{
		{
			desc:                  "call 1 succeeds, so second call succeeds without checks",
			readinessCacheTTL:     10 * time.Minute,
			waitInvalidation:      false,
			wantFirstCallSuccess:  true,
			wantSecondCallSuccess: true,
			setupMocks: func(m *serverMocks) {
				m.sched.EXPECT().Probe().Return(nil)
				m.tags.EXPECT().CheckReadiness().Return(nil)
				m.ac.EXPECT().CheckReadiness().Return(nil)
			},
		},
		{
			desc:                  "call 1 fails, so second call performs checks",
			readinessCacheTTL:     10 * time.Minute,
			waitInvalidation:      false,
			wantFirstCallSuccess:  false,
			wantSecondCallSuccess: true,
			setupMocks: func(m *serverMocks) {
				m.sched.EXPECT().Probe().Return(testErr)
				m.tags.EXPECT().CheckReadiness().Return(testErr)
				m.ac.EXPECT().CheckReadiness().Return(testErr)

				m.sched.EXPECT().Probe().Return(nil)
				m.tags.EXPECT().CheckReadiness().Return(nil)
				m.ac.EXPECT().CheckReadiness().Return(nil)
			},
		},
		{
			desc:                  "call 1 succeeds, but cache becomes invalid, so second call performs checks",
			readinessCacheTTL:     10 * time.Minute,
			waitInvalidation:      true,
			wantFirstCallSuccess:  true,
			wantSecondCallSuccess: false,
			setupMocks: func(m *serverMocks) {
				m.sched.EXPECT().Probe().Return(nil)
				m.tags.EXPECT().CheckReadiness().Return(nil)
				m.ac.EXPECT().CheckReadiness().Return(nil)

				m.sched.EXPECT().Probe().Return(testErr)
				m.tags.EXPECT().CheckReadiness().Return(testErr)
				m.ac.EXPECT().CheckReadiness().Return(testErr)
			},
		},
		{
			desc:                  "call 1 succeeds, but caching is disabled, so second call performs checks",
			readinessCacheTTL:     0,
			waitInvalidation:      false,
			wantFirstCallSuccess:  true,
			wantSecondCallSuccess: false,
			setupMocks: func(m *serverMocks) {
				m.sched.EXPECT().Probe().Return(nil)
				m.tags.EXPECT().CheckReadiness().Return(nil)
				m.ac.EXPECT().CheckReadiness().Return(nil)

				m.sched.EXPECT().Probe().Return(testErr)
				m.tags.EXPECT().CheckReadiness().Return(testErr)
				m.ac.EXPECT().CheckReadiness().Return(testErr)
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require := require.New(t)
			mocks, cleanup := newServerMocks(t)
			defer cleanup()

			tc.setupMocks(mocks)

			s, addr := mocks.startServer(Config{readinessCacheTTL: tc.readinessCacheTTL})
			_, err := httputil.Get(fmt.Sprintf("http://%s/readiness", addr))
			if tc.wantFirstCallSuccess {
				require.NoError(err)
			} else {
				require.Error(err)
			}

			if tc.waitInvalidation {
				// To avoid using time.Sleep, we can rollback the server's lastReady variable to simulate cache invalidation.
				s.lastReady = s.lastReady.Add(-1 * tc.readinessCacheTTL)
			}

			_, err = httputil.Get(fmt.Sprintf("http://%s/readiness", addr))
			if tc.wantSecondCallSuccess {
				require.NoError(err)
			} else {
				require.Error(err)
			}
		})
	}
}

func TestPatchSchedulerConfigHandler(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	_, addr := mocks.startServer(Config{})

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

	_, addr := mocks.startServer(Config{})

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

	_, addr := mocks.startServer(Config{})

	mocks.sched.EXPECT().RemoveTorrent(d).Return(nil)

	_, err := httputil.Delete(fmt.Sprintf("http://%s/blobs/%s", addr, d))
	require.NoError(err)
}

func TestPreloadHandler(t *testing.T) {
	tag := url.PathEscape("repo1:tag1")
	tests := []struct {
		name          string
		url           string
		setup         func(*serverMocks)
		expectedError string
	}{
		{
			name: "success docker",
			url:  fmt.Sprintf("/preload/tags/%s", tag),
			setup: func(mocks *serverMocks) {
				mocks.dockerCli.EXPECT().
					PullImage(context.Background(), "repo1", "tag1").Return(nil)
				mocks.containerRuntime.EXPECT().
					DockerClient().Return(mocks.dockerCli)
			},
		},
		{
			name: "success containerd",
			url:  fmt.Sprintf("/preload/tags/%s?runtime=containerd&namespace=name.space1", tag),
			setup: func(mocks *serverMocks) {
				mocks.containerdCli.EXPECT().
					PullImage(context.Background(), "name.space1", "repo1", "tag1").Return(nil)
				mocks.containerRuntime.EXPECT().
					ContainerdClient().Return(mocks.containerdCli)
			},
		},
		{
			name:          "unsupported runtime",
			url:           fmt.Sprintf("/preload/tags/%s?runtime=crio", tag),
			setup:         func(_ *serverMocks) {},
			expectedError: "/preload/tags/repo1:tag1?runtime=crio 500: unsupported container runtime",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			mocks, cleanup := newServerMocks(t)
			defer cleanup()

			tt.setup(mocks)
			_, addr := mocks.startServer(Config{})

			_, err := httputil.Get(fmt.Sprintf("http://%s%s", addr, tt.url))
			if tt.expectedError != "" {
				require.EqualError(err,
					fmt.Sprintf("GET http://%s%s", addr, tt.expectedError))
			} else {
				require.NoError(err)
			}
		})
	}
}
