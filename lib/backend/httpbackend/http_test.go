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
package httpbackend

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/utils/memsize"
	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/require"
)

func TestClientFactory(t *testing.T) {
	require := require.New(t)

	config := Config{}
	f := factory{}
	_, err := f.Create(config, nil)
	require.NoError(err)
}

func TestHttpDownloadSuccess(t *testing.T) {
	require := require.New(t)

	blob := randutil.Blob(32 * memsize.KB)

	r := chi.NewRouter()
	r.Get("/data/{blob}", func(w http.ResponseWriter, req *http.Request) {
		_, err := io.Copy(w, bytes.NewReader(blob))
		require.NoError(err)
	})
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := Config{DownloadURL: "http://" + addr + "/data/%s"}
	client, err := NewClient(config)
	require.NoError(err)

	var b bytes.Buffer
	require.NoError(client.Download(core.NamespaceFixture(), "data", &b))
	require.Equal(blob, b.Bytes())
}

func TestHttpDownloadFileNotFound(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()
	r.Get("/data/{blob}", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	})
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := Config{DownloadURL: "http://" + addr + "/data/%s"}
	client, err := NewClient(config)
	require.NoError(err)

	var b bytes.Buffer
	require.Equal(backenderrors.ErrBlobNotFound, client.Download(core.NamespaceFixture(), "data", &b))
}

func TestDownloadMalformedURLThrowsError(t *testing.T) {
	require := require.New(t)

	// Empty router.
	addr, stop := testutil.StartServer(chi.NewRouter())
	defer stop()

	config := Config{DownloadURL: "http://" + addr + "/data"}
	client, err := NewClient(config)
	require.NoError(err)

	var b bytes.Buffer
	require.Error(client.Download(core.NamespaceFixture(), "data", &b))
}
