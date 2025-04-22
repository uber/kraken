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
	"fmt"
	"github.com/docker/distribution"
	ms "github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/opencontainers/go-digest"
	"io/ioutil"
	"net/http"
	"net/url"
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

func TestPrefetchWithoutTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	_, err := httputil.Post(fmt.Sprintf("http://%s/proxy/v1/registry/prefetch", addr))
	require.Error(err)
	require.True(httputil.IsStatus(err, http.StatusBadRequest))
}

func TestPrefetchMalformedTag(t *testing.T) {
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

func TestPrefetch(t *testing.T) {
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

	tagReq := url.QueryEscape(fmt.Sprintf("%s/%s", namespace, tag))
	mocks.tagClient.EXPECT().Get(tagReq).Return(manifest, nil)
	mocks.originClient.EXPECT().DownloadBlob(namespace, manifest, mockutil.MatchWriter(bs)).Return(nil)
	mocks.originClient.EXPECT().DownloadBlob(namespace, layers[1], ioutil.Discard).Return(nil)
	mocks.originClient.EXPECT().DownloadBlob(namespace, layers[2], ioutil.Discard).Return(nil)
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/proxy/v1/registry/prefetch", addr),
		httputil.SendBody(bytes.NewReader(b)))
	require.NoError(err)
}

func TestPrefetchError(t *testing.T) {
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

	tagReq := url.QueryEscape(fmt.Sprintf("%s/%s", namespace, tag))
	mocks.tagClient.EXPECT().Get(tagReq).Return(manifest, nil)
	mocks.originClient.EXPECT().DownloadBlob(namespace, manifest, mockutil.MatchWriter(bs)).Return(nil)
	mocks.originClient.EXPECT().DownloadBlob(namespace, layers[1], ioutil.Discard).Return(fmt.Errorf("error"))
	mocks.originClient.EXPECT().DownloadBlob(namespace, layers[2], ioutil.Discard).Return(nil)
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/proxy/v1/registry/prefetch", addr),
		httputil.SendBody(bytes.NewReader(b)))
	require.NoError(err)
}

func toBytes(list manifestlist.ManifestList) ([]byte, error) {
	data, err := json.Marshal(list)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func TestPrefetchManifestList(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr := mocks.startServer()

	repo := "kraken-test"
	namespace := "preheat"
	tag := "abcdef:v1.0.0"

	layers1 := core.DigestListFixture(3)
	layers2 := core.DigestListFixture(3)
	manifest1, bs1 := dockerutil.ManifestFixture(layers1[0], layers1[1], layers1[2])
	manifest2, bs2 := dockerutil.ManifestFixture(layers2[0], layers2[1], layers2[2])
	descs := []manifestlist.ManifestDescriptor{
		{
			Descriptor: distribution.Descriptor{
				Digest: digest.NewDigestFromHex(manifest1.Algo(), manifest1.Hex()),
			},
		},
		{
			Descriptor: distribution.Descriptor{
				Digest: digest.NewDigestFromHex(manifest2.Algo(), manifest2.Hex()),
			},
		},
	}
	manifestList := manifestlist.ManifestList{
		Versioned: ms.Versioned{
			SchemaVersion: 2,
			MediaType:     "application/vnd.docker.distribution.manifest.list.v2+json",
		},
		Manifests: descs,
	}

	b, _ := json.Marshal(prefetchBody{
		Tag:     fmt.Sprintf("%s/%s/%s", repo, namespace, tag),
		TraceId: "abc",
	})

	raw, err := toBytes(manifestList)
	d, err := core.NewDigester().FromBytes(raw)
	if err != nil {
		panic(err)
	}

	d1, _ := core.NewSHA256DigestFromHex(descs[0].Digest.Hex())
	d2, _ := core.NewSHA256DigestFromHex(descs[1].Digest.Hex())

	tagReq := url.QueryEscape(fmt.Sprintf("%s/%s", namespace, tag))
	mocks.tagClient.EXPECT().Get(tagReq).Return(d, nil)
	mocks.originClient.EXPECT().DownloadBlob(namespace, d, mockutil.MatchWriter(raw)).Return(nil)
	mocks.originClient.EXPECT().DownloadBlob(namespace, d1, mockutil.MatchWriter(bs1)).Return(nil)
	mocks.originClient.EXPECT().DownloadBlob(namespace, d2, mockutil.MatchWriter(bs2)).Return(nil)
	mocks.originClient.EXPECT().DownloadBlob(namespace, layers1[1], ioutil.Discard).Return(nil)
	mocks.originClient.EXPECT().DownloadBlob(namespace, layers1[2], ioutil.Discard).Return(nil)

	mocks.originClient.EXPECT().DownloadBlob(namespace, layers2[1], ioutil.Discard).Return(nil)
	mocks.originClient.EXPECT().DownloadBlob(namespace, layers2[2], ioutil.Discard).Return(nil)
	_, err = httputil.Post(
		fmt.Sprintf("http://%s/proxy/v1/registry/prefetch", addr),
		httputil.SendBody(bytes.NewReader(b)))
	require.NoError(err)
}
