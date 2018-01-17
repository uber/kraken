package httpbackend

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
)

func TestHttpDownloadFileSuccess(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()

	// generate 32KB of random data
	b := randutil.Blob(32 * memsize.KB)
	buf := bytes.NewBuffer(b)

	r.Get("/data/:blob", func(w http.ResponseWriter, req *http.Request) {
		_, err := io.Copy(w, buf)
		if err != nil {
			panic(err)
		}
	})

	addr, close := testutil.StartServer(r)
	defer close()

	config := &Config{
		DownloadURL: "http://" + addr + "/data/%s",
	}
	httpc, err := NewClient(*config)
	require.NoError(err)

	f, err := ioutil.TempFile("", "httptest")
	require.NoError(err)
	defer os.Remove(f.Name())

	err = httpc.Download("data", f)
	require.NoError(err)

	bd, err := ioutil.ReadFile(f.Name())
	require.Equal(bytes.Compare(b, bd), 0)
}

func TestHttpDownloadFileNotFound(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()

	ncalled := false
	r.Get("/data/:blob", func(w http.ResponseWriter, req *http.Request) {
		ncalled = true
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	})

	addr, close := testutil.StartServer(r)
	defer close()

	config := &Config{
		DownloadURL: "http://" + addr + "/data/%s",
	}
	httpc, err := NewClient(*config)
	require.NoError(err)

	f, err := ioutil.TempFile("", "httptest")
	require.NoError(err)
	defer os.Remove(f.Name())

	err = httpc.Download("data", f)
	require.Equal(backenderrors.ErrBlobNotFound, err)

	require.True(ncalled)
}

func TestDownloadMalformedUrlThrowsError(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()

	addr, close := testutil.StartServer(r)
	defer close()

	config := &Config{
		DownloadURL: "http://" + addr + "/data",
	}

	httpc, err := NewClient(*config)
	require.NoError(err)

	f, err := ioutil.TempFile("", "httptest")
	require.NoError(err)
	defer os.Remove(f.Name())

	err = httpc.Upload("data", f)
	require.Error(err)
}
