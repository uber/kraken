package registrybackend

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

func TestBlobDownloadSuccess(t *testing.T) {
	require := require.New(t)

	blob := randutil.Blob(32 * memsize.KB)
	namespace := core.NamespaceFixture()

	r := chi.NewRouter()
	r.Get(fmt.Sprintf("/v2/%s/blobs/:blob", namespace), func(w http.ResponseWriter, req *http.Request) {
		_, err := io.Copy(w, bytes.NewReader(blob))
		require.NoError(err)
	})
	r.Head(fmt.Sprintf("/v2/%s/blobs/:blob", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(blob)))
	})
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

	r := chi.NewRouter()
	r.Get(fmt.Sprintf("/v2/%s/blobs/:blob", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	})
	r.Head(fmt.Sprintf("/v2/%s/blobs/:blob", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	})
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
