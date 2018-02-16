package httpbackend

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
)

func TestHttpDownloadSuccess(t *testing.T) {
	require := require.New(t)

	blob := randutil.Blob(32 * memsize.KB)

	r := chi.NewRouter()
	r.Get("/data/:blob", func(w http.ResponseWriter, req *http.Request) {
		_, err := io.Copy(w, bytes.NewReader(blob))
		require.NoError(err)
	})
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := Config{DownloadURL: "http://" + addr + "/data/%s"}
	client, err := NewClient(config)
	require.NoError(err)

	var b bytes.Buffer
	require.NoError(client.Download("data", &b))
	require.Equal(blob, b.Bytes())
}

func TestHttpDownloadFileNotFound(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()
	r.Get("/data/:blob", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	})
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := Config{DownloadURL: "http://" + addr + "/data/%s"}
	client, err := NewClient(config)
	require.NoError(err)

	var b bytes.Buffer
	require.Equal(backenderrors.ErrBlobNotFound, client.Download("data", &b))
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
	require.Error(client.Download("data", &b))
}
