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
package artifactorybackend

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/utils/memsize"
	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/testutil"
)

func TestClientFactory(t *testing.T) {
	require := require.New(t)

	config := Config{}
	f := blobClientFactory{}
	_, err := f.Create(config, nil)
	require.NoError(err)
}

func addTokenRoute(r *chi.Mux, require *require.Assertions) {
	r.Get("/v2/token", func(w http.ResponseWriter, req *http.Request) {
		_, err := w.Write([]byte(`{"token":"123456789","expires_in":3600}`))
		require.NoError(err)
	})
}

func createNewBlobsRouter(namespace string, blob []byte, require *require.Assertions, valid bool) (r *chi.Mux) {
	r = chi.NewRouter()
	addTokenRoute(r, require)
	r.Get(fmt.Sprintf("/v2/%s/blobs/{blob}", namespace), func(w http.ResponseWriter, req *http.Request) {
		if valid {
			_, err := io.Copy(w, bytes.NewReader(blob))
			require.NoError(err)
		} else {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("file not found"))
		}
	})
	r.Head(fmt.Sprintf("/v2/%s/blobs/{blob}", namespace), func(w http.ResponseWriter, req *http.Request) {
		if valid {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(blob)))
		} else {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("file not found"))
		}
	})

	return
}

func createNewManifestRouter(namespace string, blob []byte, require *require.Assertions) (r *chi.Mux) {
	r = chi.NewRouter()
	addTokenRoute(r, require)
	r.Get(fmt.Sprintf("/v2/%s/manifests/{blob}", namespace), func(w http.ResponseWriter, req *http.Request) {
		_, err := io.Copy(w, bytes.NewReader(blob))
		require.NoError(err)
	})
	r.Head(fmt.Sprintf("/v2/%s/manifests/{blob}", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(blob)))
	})

	return
}

func TestBlobDownloadBlobSuccess(t *testing.T) {
	require := require.New(t)

	blob := randutil.Blob(32 * memsize.KB)
	namespace := core.NamespaceFixture()
	r := createNewBlobsRouter(namespace, blob, require, true)
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := Config{Address: addr}
	client, err := NewBlobClient(config)
	require.NoError(err)

	info, err := client.Stat(namespace, "data")
	require.NoError(err)
	require.Equal(int64(len(blob)), info.Size)

	var b bytes.Buffer
	require.NoError(client.Download(namespace, "data", &b))
	require.Equal(blob, b.Bytes())
}

func TestBlobDownloadManifestSuccess(t *testing.T) {
	require := require.New(t)

	blob := randutil.Blob(32 * memsize.KB)
	namespace := core.NamespaceFixture()

	r := createNewManifestRouter(namespace, blob, require)
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := Config{Address: addr}
	client, err := NewBlobClient(config)
	require.NoError(err)

	info, err := client.Stat(namespace, "data")
	require.NoError(err)
	require.Equal(int64(len(blob)), info.Size)

	var b bytes.Buffer
	require.NoError(client.Download(namespace, "data", &b))
	require.Equal(blob, b.Bytes())
}

func TestBlobDownloadFileNotFound(t *testing.T) {
	require := require.New(t)

	namespace := core.NamespaceFixture()

	r := createNewBlobsRouter(namespace, nil, require, false)
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := Config{Address: addr}
	client, err := NewBlobClient(config)
	require.NoError(err)

	_, err = client.Stat(namespace, "data")
	require.Equal(backenderrors.ErrBlobNotFound, err)

	var b bytes.Buffer
	require.Equal(backenderrors.ErrBlobNotFound, client.Download(namespace, "data", &b))
}
