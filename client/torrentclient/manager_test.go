package torrentclient

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/configuration"

	"github.com/stretchr/testify/require"
)

func getFileStore() (*configuration.Config, *store.LocalStore) {
	cp := configuration.GetConfigFilePath("agent/test.yaml")
	c := configuration.NewConfigWithPath(cp)
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

func removeTestTorrentDirs(c *configuration.Config) {
	os.RemoveAll(c.DownloadDir)
	os.RemoveAll(c.CacheDir)
	os.RemoveAll(c.UploadDir)
	os.Remove("./.torrents.list.bolt.db")
}

func TestNewManager(t *testing.T) {
	assert := require.New(t)
	c, s := getFileStore()
	defer removeTestTorrentDirs(c)
	m := NewManager(c, s)
	assert.Nil(m.Close())
}
