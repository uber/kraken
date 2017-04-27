package store

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/configuration"

	"fmt"

	"github.com/stretchr/testify/assert"
)

func GetTestFileStore() (*configuration.Config, *LocalFileStore) {
	cp := configuration.GetConfigFilePath("agent/test.yaml")
	c := configuration.NewConfigWithPath(cp)
	c.DisableTorrent = true
	c.TagDeletion = struct {
		Enable         bool `yaml:"enable"`
		Interval       int  `yaml:"interval"`
		RetentionCount int  `yaml:"retention_count"`
		RetentionTime  int  `yaml:"retention_time"`
	}{
		Enable:         true,
		RetentionCount: 10,
	}
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
	err = os.MkdirAll(c.TagDir, 0755)
	if err != nil {
		log.Fatal(err)
	}
	c.UploadDir, err = ioutil.TempDir(c.UploadDir, "testtags")
	if err != nil {
		log.Fatal(err)
	}
	c.CacheDir, err = ioutil.TempDir(c.CacheDir, "testtags")
	if err != nil {
		log.Fatal(err)
	}
	c.DownloadDir, err = ioutil.TempDir(c.DownloadDir, "testtags")
	if err != nil {
		log.Fatal(err)
	}
	c.TagDir, err = ioutil.TempDir(c.TagDir, "testtags")
	if err != nil {
		log.Fatal(err)
	}
	s := NewLocalFileStore(c)
	return c, s
}

func cleanupTestFileStore(c *configuration.Config) {
	os.RemoveAll(c.DownloadDir)
	os.RemoveAll(c.CacheDir)
	os.RemoveAll(c.UploadDir)
	os.RemoveAll(c.TagDir)
}

func TestDownloadAndDeleteFiles(t *testing.T) {
	c, s := GetTestFileStore()
	defer cleanupTestFileStore(c)

	var waitGroup sync.WaitGroup

	for i := 0; i < 100; i++ {
		waitGroup.Add(1)

		testFileName := fmt.Sprintf("test_%d", i)
		go func() {
			created, err := s.CreateDownloadFile(testFileName, 1)
			assert.True(t, created)
			assert.Nil(t, err)
			err = s.MoveDownloadFileToCache(testFileName)
			assert.Nil(t, err)
			err = s.MoveCacheFileToTrash(testFileName)
			assert.Nil(t, err)

			waitGroup.Done()
		}()
	}

	waitGroup.Wait()
	err := s.DeleteAllTrashFiles()
	assert.Nil(t, err)

	for i := 0; i < 100; i++ {
		testFileName := fmt.Sprintf("test_%d", i)
		_, err := os.Stat(path.Join(stateTrash.GetDirectory(), testFileName))
		assert.True(t, os.IsNotExist(err))
	}
}
