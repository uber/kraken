package hdfsbackend

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
)

func TestHDFSDownloadFileSuccess(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()

	ncalled := false
	r.Get("/*/:shard/:blob/data", func(w http.ResponseWriter, req *http.Request) {
		ncalled = true
		url := fmt.Sprintf(
			"http://%s/datanode", req.Host)

		http.Redirect(w, req, url, http.StatusTemporaryRedirect)
	})

	dncalled := false
	r.Get("/datanode", func(w http.ResponseWriter, r *http.Request) {
		dncalled = true
		w.Write([]byte{})
	})

	addr, stop := testutil.StartServer(r)
	defer stop()

	config := &Config{
		NameNodeRoundRobin: serverset.RoundRobinConfig{
			Addrs:   []string{addr},
			Retries: 3,
		},
		BuffSize: int64(64 * memsize.MB)}
	hdfsc, err := NewHDFSClient(*config)
	require.NoError(err)

	f, err := ioutil.TempFile("", "hdfs3test")

	require.NoError(err)
	defer os.Remove(f.Name())

	d, _ := image.DigestWithBlobFixture()

	err = hdfsc.Download(d.Hex(), f)
	require.NoError(err)

	require.True(ncalled)
	require.True(dncalled)
}

func TestHDFSDownloadRetryNext(t *testing.T) {
	require := require.New(t)

	r1 := chi.NewRouter()
	r2 := chi.NewRouter()

	r1.Get("/*/:shard/:blob/data", func(w http.ResponseWriter, req *http.Request) {
		url := fmt.Sprintf(
			"http://%s/datanode", req.Host)

		http.Redirect(w, req, url, http.StatusTemporaryRedirect)
	})

	r1.Get("/datanode", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte{})
	})

	r2.Get("/*/:shard/:blob/data", func(w http.ResponseWriter, req *http.Request) {
		url := fmt.Sprintf(
			"http://%s/datanode", req.Host)

		http.Redirect(w, req, url, http.StatusTemporaryRedirect)
	})

	r2.Get("/datanode", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("test"))
	})

	addr1, stop1 := testutil.StartServer(r1)
	defer stop1()

	addr2, stop2 := testutil.StartServer(r2)
	defer stop2()

	config := &Config{
		NameNodeRoundRobin: serverset.RoundRobinConfig{
			Addrs:   []string{addr1, addr2},
			Retries: 3,
		},
		BuffSize: int64(64 * memsize.MB)}
	hdfsc, err := NewHDFSClient(*config)
	require.NoError(err)

	f, err := ioutil.TempFile("", "hdfs3test")

	require.NoError(err)
	defer os.Remove(f.Name())

	d, _ := image.DigestWithBlobFixture()

	err = hdfsc.Download(d.Hex(), f)
	require.NoError(err)

	data, err := ioutil.ReadFile(f.Name())
	require.NoError(err)

	require.Equal("test", string(data))
}

func TestHDFSDownloadFileNotFound(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()

	ncalled := false
	r.Get("/*/:shard/:blob/data", func(w http.ResponseWriter, req *http.Request) {
		ncalled = true
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	})

	dncalled := false
	r.Get("/datanode", func(w http.ResponseWriter, r *http.Request) {
		dncalled = true
		w.Write([]byte{})
	})

	addr, stop := testutil.StartServer(r)
	defer stop()

	config := &Config{
		NameNodeRoundRobin: serverset.RoundRobinConfig{
			Addrs:   []string{addr},
			Retries: 3,
		},
		BuffSize: int64(64 * memsize.MB)}

	hdfsc, err := NewHDFSClient(*config)
	require.NoError(err)

	f, err := ioutil.TempFile("", "hdfs3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	d, _ := image.DigestWithBlobFixture()

	err = hdfsc.Download(d.Hex(), f)
	require.Error(err)

	require.True(ncalled)
	require.True(!dncalled)
}

func TestHDFSUploadFileSuccess(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()

	ncalled := false
	r.Put("/*/:blob/data", func(w http.ResponseWriter, req *http.Request) {
		ncalled = true
		url := fmt.Sprintf("http://%s/datanode", req.Host)
		http.Redirect(w, req, url, http.StatusTemporaryRedirect)
	})

	dncalled := false
	r.Put("/datanode", func(w http.ResponseWriter, r *http.Request) {
		dncalled = true
		w.WriteHeader(http.StatusCreated)
	})

	addr, stop := testutil.StartServer(r)
	defer stop()

	config := &Config{
		NameNodeRoundRobin: serverset.RoundRobinConfig{
			Addrs:   []string{addr},
			Retries: 3,
		},
		BuffSize: int64(64 * memsize.MB)}

	hdfsc, err := NewHDFSClient(*config)
	require.NoError(err)

	f, err := ioutil.TempFile("", "hdfs3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	d, _ := image.DigestWithBlobFixture()
	err = hdfsc.Upload(d.Hex(), f)

	require.NoError(err)

	require.True(ncalled)
	require.True(dncalled)
}

func TestHDFSUploadFileUnknownFailure(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()

	ncalled := false
	r.Put("/*/:blob/data", func(w http.ResponseWriter, req *http.Request) {
		ncalled = true
		url := fmt.Sprintf("http://%s/datanode", req.Host)
		http.Redirect(w, req, url, http.StatusTemporaryRedirect)
	})

	dncalled := false
	r.Put("/datanode", func(w http.ResponseWriter, r *http.Request) {
		dncalled = true

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("unknown error"))
	})

	addr, stop := testutil.StartServer(r)
	defer stop()

	config := &Config{
		NameNodeRoundRobin: serverset.RoundRobinConfig{
			Addrs:   []string{addr},
			Retries: 3,
		},
		BuffSize: int64(64 * memsize.MB)}

	hdfsc, err := NewHDFSClient(*config)
	require.NoError(err)

	f, err := ioutil.TempFile("", "hdfs3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	d, _ := image.DigestWithBlobFixture()
	err = hdfsc.Upload(d.Hex(), f)
	require.Error(err)

	require.True(ncalled)
	require.True(dncalled)
}

func TestHDFSGetManifestSuccess(t *testing.T) {
	require := require.New(t)

	r := chi.NewRouter()

	d, _ := image.DigestWithBlobFixture()

	r.Get("/*/:repo/:tag/data", func(w http.ResponseWriter, req *http.Request) {
		url := fmt.Sprintf("http://%s/datanode", req.Host)
		http.Redirect(w, req, url, http.StatusTemporaryRedirect)
	})

	r.Get("/datanode", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("sha256:" + d.Hex()))
	})

	addr, stop := testutil.StartServer(r)
	defer stop()

	config := &Config{
		NameNodeRoundRobin: serverset.RoundRobinConfig{
			Addrs:   []string{addr},
			Retries: 3,
		},
		BuffSize: int64(64 * memsize.MB)}

	hdfsc, err := NewHDFSClient(*config)
	require.NoError(err)

	mr, err := hdfsc.GetManifest("testrepo", "testtag")
	require.NoError(err)

	data, err := ioutil.ReadAll(mr)
	require.NoError(err)

	require.Equal("sha256:"+d.Hex(), string(data))
}
