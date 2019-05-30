// Copyright (c) 2016-2019 Uber Technologies, Inc.
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
package proxyserver

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/httputil"

	"bytes"
	"encoding/json"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/mockutil"
)

func TestHealth(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/health", addr))
	defer resp.Body.Close()
	require.NoError(err)
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(err)
	require.Equal("OK\n", string(b))
}

func TestPreheatInvalidEventBody(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	_, err := httputil.Post(fmt.Sprintf("http://%s/registry/notifications", addr))
	require.Error(err)
	require.True(httputil.IsStatus(err, http.StatusInternalServerError))
}

func TestPreheatNoPushManifestEvent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	b, _ := json.Marshal(Notification{
		Events: []Event{
			{
				ID:        "1",
				TimeStamp: time.Now(),
				Action:    "pull",
				Target: &Target{
					MediaType:  "application/vnd.docker.distribution.manifest.v2+json",
					Digest:     "sha256:ce8aaa9fe90ad0dada1d2b2fed22ee9bb64bcfc1a5a4d5f7d2fe392df35050aa",
					Repository: "kraken-test/preheat",
					Tag:        "v1.0.0",
				},
			},
			{
				ID:        "2",
				TimeStamp: time.Now(),
				Action:    "push",
				Target: &Target{
					MediaType:  "application/octet-stream",
					Digest:     "sha256:ea8dbfc366942e2c856ee1cf59982ff6e08d12af3eb0a2cbf22b8eb12b43c22a",
					Repository: "kraken-test/preheat",
					Tag:        "v1.0.1",
				},
			},
		},
	})

	_, err := httputil.Post(
		fmt.Sprintf("http://%s/registry/notifications", addr),
		httputil.SendBody(bytes.NewReader(b)))
	require.NoError(err)
}

func TestPreheat(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	repo := "kraken-test/preheat"
	tag := "v1.0.0"
	layers := core.DigestListFixture(3)
	manifest, bs := dockerutil.ManifestFixture(layers[0], layers[1], layers[2])

	notification := &Notification{
		Events: []Event{
			{
				ID:        "1",
				TimeStamp: time.Now(),
				Action:    "push",
				Target: &Target{
					MediaType:  "application/vnd.docker.distribution.manifest.v2+json",
					Digest:     manifest.String(),
					Repository: repo,
					Tag:        tag,
				},
			},
		},
	}

	b, _ := json.Marshal(notification)

	mocks.originClient.EXPECT().DownloadBlob(repo, manifest, mockutil.MatchWriter(bs)).Return(nil)
	mocks.originClient.EXPECT().GetMetaInfo(repo, layers[0]).Return(nil, nil)
	mocks.originClient.EXPECT().GetMetaInfo(repo, layers[1]).Return(nil, nil)
	mocks.originClient.EXPECT().GetMetaInfo(repo, layers[2]).Return(nil, nil)
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/registry/notifications", addr),
		httputil.SendBody(bytes.NewReader(b)))
	require.NoError(err)
}
