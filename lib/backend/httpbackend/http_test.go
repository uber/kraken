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
package httpbackend

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/memsize"
	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestClientFactory(t *testing.T) {
	require := require.New(t)

	config := Config{}
	f := factory{}
	_, err := f.Create(config, nil, tally.NoopScope, zap.NewNop().Sugar())
	require.NoError(err)
}

func TestClientFactoryConfigError(t *testing.T) {
	f := factory{}

	// A config that cannot be marshaled, so the factory must name the http backend
	// and keep the underlying error.
	_, err := f.Create(failingConfig{}, nil, tally.NoopScope, zap.NewNop().Sugar())
	require.Error(t, err)
	require.Contains(t, err.Error(), "marshal http config")
	require.NotNil(t, errors.Unwrap(err))

	// A config that cannot be unmarshaled, so the factory must name the http
	// backend and keep the underlying error.
	_, err = f.Create([]string{"not", "a", "mapping"}, nil, tally.NoopScope, zap.NewNop().Sugar())
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal http config")
	require.NotNil(t, errors.Unwrap(err))
}

// failingConfig makes yaml.Marshal return an error through the Marshaler
// interface. Unsupported types (channels, funcs) cannot be used here: yaml.v2
// panics on those instead of returning an error.
type failingConfig struct{}

func (failingConfig) MarshalYAML() (interface{}, error) {
	return nil, errors.New("marshal failure")
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
	client, err := NewClient(config, tally.NoopScope)
	require.NoError(err)
	defer closers.Close(client)

	var b bytes.Buffer
	require.NoError(client.Download(core.NamespaceFixture(), "data", &b))
	require.Equal(blob, b.Bytes())
}

func TestHttpDownloadFileNotFound(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()
	r.Get("/data/{blob}", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, err := w.Write([]byte("file not found"))
		require.NoError(err)
	})
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := Config{DownloadURL: "http://" + addr + "/data/%s"}
	client, err := NewClient(config, tally.NoopScope)
	require.NoError(err)
	defer closers.Close(client)

	var b bytes.Buffer
	require.Equal(backenderrors.ErrBlobNotFound, client.Download(core.NamespaceFixture(), "data", &b))
}

func TestDownloadMalformedURLThrowsError(t *testing.T) {
	require := require.New(t)

	// Empty router.
	addr, stop := testutil.StartServer(chi.NewRouter())
	defer stop()

	config := Config{DownloadURL: "http://" + addr + "/data"}
	client, err := NewClient(config, tally.NoopScope)
	require.NoError(err)
	defer closers.Close(client)

	var b bytes.Buffer
	require.Error(client.Download(core.NamespaceFixture(), "data", &b))
}
