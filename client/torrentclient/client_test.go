package torrentclient

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"code.uber.internal/go-common.git/x/log"
	"github.com/pressly/chi"
	"github.com/stretchr/testify/assert"
)

const (
	successrepo = "successrepo"
	successtag  = "tag"
)

var _server *httptest.Server

func getTestRouter() *chi.Mux {
	r := chi.NewRouter()
	r.HandleFunc("/manifest/:name", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Info("request")
		name := chi.URLParam(r, "name")
		name, _ = url.QueryUnescape(name)
		if name == "successrepo:tag" {
			data, err := ioutil.ReadFile("../dockerregistry/test/testmanifest.json")
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(name + " not found"))
	}))
	return r
}

func TestPostGetManifest(t *testing.T) {
	config, store := getFileStore()
	defer removeTestTorrentDirs(config)

	_server = httptest.NewServer(getTestRouter())
	defer _server.Close()
	config.TrackerURL = _server.URL
	cli, err := NewClient(config, store, 120)
	assert.Nil(t, err)

	repo := successrepo
	tag := successtag
	manifest := "testmanifest"
	manifestTemp := manifest + ".tmp"

	// manifest not exist in cache
	err = cli.PostManifest("repo", "tag", manifest)
	assert.NotNil(t, err)
	assert.True(t, os.IsNotExist(err))

	cli.store.CreateUploadFile(manifestTemp, 0)
	writer, _ := cli.store.GetUploadFileReadWriter(manifestTemp)
	writer.Write([]byte("this is a manifest content"))
	writer.Close()
	cli.store.MoveUploadFileToCache(manifestTemp, manifest)

	// post
	// success
	err = cli.PostManifest(repo, tag, manifest)
	assert.Nil(t, err)

	// fail
	err = cli.PostManifest("failedrepo", tag, manifest)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "failedrepo:tag not found"))

	// get
	// success
	digest, err := cli.GetManifest(repo, tag)
	assert.Nil(t, err)
	assert.Equal(t, "09b4be55821450cbf046f7ed71c7a1d9512b442c7967004651f7bff084a285c1", digest)
	reader, _ := cli.store.GetCacheFileReader("09b4be55821450cbf046f7ed71c7a1d9512b442c7967004651f7bff084a285c1")
	defer reader.Close()
	data, _ := ioutil.ReadAll(reader)
	dataexpected, _ := ioutil.ReadFile("../dockerregistry/test/testmanifest.json")
	assert.Equal(t, dataexpected, data)

	// get again should not return any error
	digest, err = cli.GetManifest(repo, tag)
	assert.Nil(t, err)
	assert.Equal(t, "09b4be55821450cbf046f7ed71c7a1d9512b442c7967004651f7bff084a285c1", digest)

	// fail
	_, err = cli.GetManifest("failedrepo", tag)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "failedrepo:tag not found"))

	// disabled
	cli.config.DisableTorrent = true
	err = cli.PostManifest(repo, tag, manifest)
	assert.Nil(t, err)
	digest, err = cli.GetManifest(repo, tag)
	assert.NotNil(t, err)
	assert.Equal(t, "Torrent disabled", err.Error())
}
