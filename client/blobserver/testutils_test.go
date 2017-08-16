package blobserver

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/configuration"
)

const (
	emptyDigestHex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	randomUUID     = "b9cb2c15-3cb5-46bf-a63c-26b0c5b9bc24"
)

type mockResponseWriter struct {
	header http.Header
	status int
	buf    *bytes.Buffer
}

func newMockResponseWriter() *mockResponseWriter {
	return &mockResponseWriter{
		header: http.Header{},
		buf:    bytes.NewBuffer([]byte("")),
	}
}

func getLocalStore() (*configuration.Config, *store.LocalStore) {
	c := configuration.NewConfigWithPath("../../config/agent/test.yaml")
	var err error
	err = os.MkdirAll(c.DownloadDir, 0755)
	if err != nil {
		log.Fatal(err)
	}
	err = os.MkdirAll(c.CacheDir, 0755)
	if err != nil {
		log.Fatal(err)
	}
	err = os.MkdirAll(c.UploadDir, 0755)
	if err != nil {
		log.Fatal(err)
	}
	c.UploadDir, err = ioutil.TempDir(c.UploadDir, "testtorrent")
	if err != nil {
		log.Fatal(err)
	}
	c.CacheDir, err = ioutil.TempDir(c.CacheDir, "testtorrent")
	if err != nil {
		log.Fatal(err)
	}
	c.DownloadDir, err = ioutil.TempDir(c.DownloadDir, "testtorrent")
	if err != nil {
		log.Fatal(err)
	}
	s := store.NewLocalStore(c)
	return c, s
}

func clearLocalStore(c *configuration.Config) {
	os.RemoveAll(c.DownloadDir)
	os.RemoveAll(c.CacheDir)
	os.RemoveAll(c.UploadDir)
}
