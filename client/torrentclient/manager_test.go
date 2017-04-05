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

func getFileStore() (*configuration.Config, *store.LocalFileStore) {
	cp := configuration.GetConfigFilePath("test.yaml")
	c := configuration.NewConfig(cp)
	var err error
	c.DownloadDir, err = ioutil.TempDir(c.DownloadDir, "testtorrent")
	if err != nil {
		log.Fatal(err)
	}
	s := store.NewLocalFileStore(c)
	return c, s
}

func TestNewManager(t *testing.T) {
	assert := require.New(t)
	c, s := getFileStore()
	defer os.RemoveAll(c.DownloadDir)
	m, err := NewManager(c, s)
	assert.Nil(err)
	assert.Nil(m.Close())
}
