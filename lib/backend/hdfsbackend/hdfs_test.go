package hdfsbackend

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
)

func TestHDFSDownloadFileSuccess(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()

	ncalled := false
	r.Get("/data", func(w http.ResponseWriter, req *http.Request) {
		ncalled = true
		url := fmt.Sprintf(
			"http://%s%s/datanode?%s", req.Host, req.URL.Path, req.URL.RawQuery)

		http.Redirect(w, req, url, http.StatusTemporaryRedirect)
	})

	dncalled := false
	r.Get("/data/datanode", func(w http.ResponseWriter, r *http.Request) {
		dncalled = true
		w.Write([]byte{})
	})

	addr, close := testutil.StartServer(r)
	defer close()

	config := &Config{
		NameNodeURL: "http://" + addr,
		BuffSize:    int64(64 * memsize.MB)}
	hdfsc := NewHDFSClient(*config)

	f, err := ioutil.TempFile("", "hdfs3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	err = hdfsc.Download("data", f)
	require.NoError(err)

	require.True(ncalled)
	require.True(dncalled)
}

func TestHDFSDownloadFileNotFound(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()

	ncalled := false
	r.Get("/data", func(w http.ResponseWriter, req *http.Request) {
		ncalled = true
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	})

	dncalled := false
	r.Get("/data/datanode", func(w http.ResponseWriter, r *http.Request) {
		dncalled = true
		w.Write([]byte{})
	})

	addr, close := testutil.StartServer(r)
	defer close()

	config := &Config{
		NameNodeURL: "http://" + addr,
		BuffSize:    int64(64 * memsize.MB)}

	hdfsc := NewHDFSClient(*config)

	f, err := ioutil.TempFile("", "hdfs3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	err = hdfsc.Download("data", f)
	require.Error(err)

	require.True(ncalled)
	require.True(!dncalled)
}

func TestHDFSUploadFileSuccess(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()

	ncalled := false
	r.Put("/data", func(w http.ResponseWriter, req *http.Request) {
		ncalled = true
		url := fmt.Sprintf("http://%s%s/datanode?%s", req.Host, req.URL.Path, req.URL.RawQuery)
		http.Redirect(w, req, url, http.StatusTemporaryRedirect)
	})

	dncalled := false
	r.Put("/data/datanode", func(w http.ResponseWriter, r *http.Request) {
		dncalled = true
		w.WriteHeader(http.StatusCreated)
	})

	addr, close := testutil.StartServer(r)
	defer close()

	config := &Config{
		NameNodeURL: "http://" + addr,
		BuffSize:    int64(64 * memsize.MB)}

	hdfsc := NewHDFSClient(*config)

	f, err := ioutil.TempFile("", "hdfs3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	err = hdfsc.Upload("data", f)

	require.NoError(err)

	require.True(ncalled)
	require.True(dncalled)
}

func TestHDFSUploadFileUnknownFailure(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()

	ncalled := false
	r.Put("/data", func(w http.ResponseWriter, req *http.Request) {
		ncalled = true
		url := fmt.Sprintf("http://%s%s/datanode?%s", req.Host, req.URL.Path, req.URL.RawQuery)
		http.Redirect(w, req, url, http.StatusTemporaryRedirect)
	})

	dncalled := false
	r.Put("/data/datanode", func(w http.ResponseWriter, r *http.Request) {
		dncalled = true

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("unknown error"))
	})

	addr, close := testutil.StartServer(r)
	defer close()

	config := &Config{
		NameNodeURL: "http://" + addr,
		BuffSize:    int64(64 * memsize.MB)}

	hdfsc := NewHDFSClient(*config)

	f, err := ioutil.TempFile("", "hdfs3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	err = hdfsc.Upload("data", f)
	require.Error(err)

	require.True(ncalled)
	require.True(dncalled)
}
