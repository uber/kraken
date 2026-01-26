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
package proxyserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/httputil"

	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/mockutil"
)

func TestHealth(t *testing.T) {
	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/health", addr))

	require.NoError(t, err)
	require.NotNil(t, resp)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "OK\n", string(b))
	require.NoError(t, resp.Body.Close())
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

	b, err := json.Marshal(Notification{
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
	require.NoError(err)

	_, err = httputil.Post(
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

	mocks.originClient.EXPECT().DownloadBlob(gomock.Any(), repo, manifest, mockutil.MatchWriter(bs)).Return(nil)
	mocks.originClient.EXPECT().GetMetaInfo(repo, layers[0]).Return(nil, nil)
	mocks.originClient.EXPECT().GetMetaInfo(repo, layers[1]).Return(nil, nil)
	mocks.originClient.EXPECT().GetMetaInfo(repo, layers[2]).Return(nil, nil)
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/registry/notifications", addr),
		httputil.SendBody(bytes.NewReader(b)))
	require.NoError(err)
}

func TestPrefetchWithoutTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	_, err := httputil.Post(fmt.Sprintf("http://%s/proxy/v1/registry/prefetch", addr))
	require.Error(err)
	require.True(httputil.IsStatus(err, http.StatusBadRequest))
}

func TestPrefetchV1MalformedTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	b, _ := json.Marshal(prefetchBody{
		TraceId: "abc",
		Tag:     "invalid",
	})

	_, err := httputil.Post(
		fmt.Sprintf("http://%s/proxy/v1/registry/prefetch", addr),
		httputil.SendBody(bytes.NewReader(b)),
	)
	require.Error(err)
	require.True(httputil.IsStatus(err, http.StatusBadRequest))
}

func TestPrefetchV1(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	repo := "kraken-test"
	namespace := "preheat"
	tag := "abcdef:v1.0.0"

	layers := core.DigestListFixture(3)
	manifest, bs := dockerutil.ManifestFixture(layers[0], layers[1], layers[2])

	b, _ := json.Marshal(prefetchBody{
		Tag:     fmt.Sprintf("%s/%s/%s", repo, namespace, tag),
		TraceId: "abc",
	})

	tagRequest := url.QueryEscape(fmt.Sprintf("%s/%s", namespace, tag))
	mocks.tagClient.EXPECT().Get(tagRequest).Return(manifest, nil)
	mocks.originClient.EXPECT().DownloadBlob(gomock.Any(), namespace, manifest, mockutil.MatchWriter(bs)).Return(nil)
	mocks.originClient.EXPECT().DownloadBlob(gomock.Any(), namespace, layers[1], io.Discard).Return(nil)
	mocks.originClient.EXPECT().DownloadBlob(gomock.Any(), namespace, layers[2], io.Discard).Return(nil)
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/proxy/v1/registry/prefetch", addr),
		httputil.SendBody(bytes.NewReader(b)))
	require.NoError(err)
}

func TestPrefetchV2(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	repo := "kraken-test"
	namespace := "preheat"
	tag := "abcdef:v1.0.0"

	layers := core.DigestListFixture(3)
	manifest, bs := dockerutil.ManifestFixture(layers[0], layers[1], layers[2])

	b, _ := json.Marshal(prefetchBody{
		Tag:     fmt.Sprintf("%s/%s/%s", repo, namespace, tag),
		TraceId: "abc",
	})

	tagRequest := url.QueryEscape(fmt.Sprintf("%s/%s", namespace, tag))
	mocks.tagClient.EXPECT().Get(tagRequest).Return(manifest, nil)
	mocks.originClient.EXPECT().DownloadBlob(gomock.Any(), namespace, manifest, mockutil.MatchWriter(bs)).Return(nil)

	mocks.originClient.EXPECT().PrefetchBlob(namespace, layers[1]).Return(nil)
	mocks.originClient.EXPECT().PrefetchBlob(namespace, layers[2]).Return(nil)
	res, err := httputil.Post(
		fmt.Sprintf("http://%s/proxy/v2/registry/prefetch", addr),
		httputil.SendBody(bytes.NewReader(b)))
	require.NoError(err)

	var resBody prefetchResponse
	resBodyBytes, err := io.ReadAll(res.Body)
	require.NoError(err)
	err = json.Unmarshal(resBodyBytes, &resBody)
	require.NoError(err)

	require.Equal(prefetchResponse{
		Message:    "prefetching initiated successfully",
		TraceId:    "abc",
		Status:     "success",
		Tag:        "abcdef:v1.0.0",
		Prefetched: true,
	}, resBody)
}

func TestPrefetchV2OriginError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	repo := "kraken-test"
	namespace := "preheat"
	tag := "abcdef:v1.0.0"

	layers := core.DigestListFixture(3)
	manifest, bs := dockerutil.ManifestFixture(layers[0], layers[1], layers[2])

	b, _ := json.Marshal(prefetchBody{
		Tag:     fmt.Sprintf("%s/%s/%s", repo, namespace, tag),
		TraceId: "abc",
	})

	tagRequest := url.QueryEscape(fmt.Sprintf("%s/%s", namespace, tag))
	mocks.tagClient.EXPECT().Get(tagRequest).Return(manifest, nil)
	mocks.originClient.EXPECT().DownloadBlob(gomock.Any(), namespace, manifest, mockutil.MatchWriter(bs)).Return(nil)

	mocks.originClient.EXPECT().PrefetchBlob(namespace, layers[1]).Return(errors.New("foo err"))
	mocks.originClient.EXPECT().PrefetchBlob(namespace, layers[2]).Return(nil)
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/proxy/v2/registry/prefetch", addr),
		httputil.SendBody(bytes.NewReader(b)))
	serr, ok := err.(httputil.StatusError)
	require.True(ok)
	require.True(httputil.IsStatus(serr, http.StatusInternalServerError))

	var resBody prefetchResponse
	err = json.Unmarshal([]byte(serr.ResponseDump), &resBody)
	require.NoError(err)
	require.Equal(prefetchResponse{
		Message:    fmt.Sprintf("failed to trigger image prefetch: at least one layer could not be prefetched: digest %q, namespace %q, blob prefetch failure: foo err", layers[1], namespace),
		TraceId:    "abc",
		Status:     "failure",
		Prefetched: false,
	}, resBody)
}
