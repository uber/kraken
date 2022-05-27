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
package registrybackend

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/testutil"
)

func TestTagDownloadSuccess(t *testing.T) {
	require := require.New(t)

	imageConfig := core.NewBlobFixture()
	layer1 := core.NewBlobFixture()
	layer2 := core.NewBlobFixture()
	digest, manifest := dockerutil.ManifestFixture(
		imageConfig.Digest, layer1.Digest, layer2.Digest)

	tag := core.TagFixture()
	namespace := strings.Split(tag, ":")[0]

	r := chi.NewRouter()
	r.Get(fmt.Sprintf("/v2/%s/manifests/{tag}", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(manifest)))
		w.Header().Set("Docker-Content-Digest", digest.String())
		_, err := io.Copy(w, bytes.NewReader(manifest))
		require.NoError(err)
	})
	r.Head(fmt.Sprintf("/v2/%s/manifests/{tag}", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(manifest)))
		w.Header().Set("Docker-Content-Digest", digest.String())
		_, err := io.Copy(w, bytes.NewReader(manifest))
		require.NoError(err)
	})
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := newTestConfig(addr)
	client, err := NewTagClient(config)
	require.NoError(err)

	info, err := client.Stat(tag, tag)
	require.NoError(err)
	require.Equal(int64(len(manifest)), info.Size)

	var b bytes.Buffer
	require.NoError(client.Download(tag, tag, &b))
	require.Equal(digest.String(), string(b.Bytes()))
}

func TestTagDownloadFileNotFound(t *testing.T) {
	require := require.New(t)

	tag := core.TagFixture()
	namespace := strings.Split(tag, ":")[0]

	r := chi.NewRouter()
	r.Get(fmt.Sprintf("/v2/%s/manifests/{tag}", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	})
	r.Head(fmt.Sprintf("/v2/%s/manifests/{tag}", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := newTestConfig(addr)
	client, err := NewTagClient(config)
	require.NoError(err)

	_, err = client.Stat(tag, tag)
	require.Equal(backenderrors.ErrBlobNotFound, err)

	var b bytes.Buffer
	require.Equal(backenderrors.ErrBlobNotFound, client.Download(tag, tag, &b))
}
