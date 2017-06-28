package store

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/configuration"

	"github.com/stretchr/testify/assert"
)

func GetTestFileStore() (*configuration.Config, *LocalStore) {
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
	err = os.MkdirAll(c.TrashDir, 0755)
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
	c.TrashDir, err = ioutil.TempDir(c.TrashDir, "testtags")
	if err != nil {
		log.Fatal(err)
	}
	c.TagDir, err = ioutil.TempDir(c.TagDir, "testtags")
	if err != nil {
		log.Fatal(err)
	}
	s := NewLocalStore(c)
	return c, s
}

func cleanupTestFileStore(c *configuration.Config) {
	os.RemoveAll(c.DownloadDir)
	os.RemoveAll(c.CacheDir)
	os.RemoveAll(c.UploadDir)
	os.RemoveAll(c.TagDir)
}

func TestFileHashStates(t *testing.T) {
	c, s := GetTestFileStore()
	defer cleanupTestFileStore(c)

	s.CreateUploadFile("test_file.txt", 100)
	err := s.SetUploadFileHashState("test_file.txt", []byte{uint8(0), uint8(1)}, "sha256", "500")
	assert.Nil(t, err)
	b, err := s.GetUploadFileHashState("test_file.txt", "sha256", "500")
	assert.Nil(t, err)
	assert.Equal(t, uint8(0), b[0])
	assert.Equal(t, uint8(1), b[1])

	l, err := s.ListUploadFileHashStatePaths("test_file.txt")
	assert.Nil(t, err)
	assert.Equal(t, len(l), 1)
	assert.True(t, strings.HasSuffix(l[0], "/hashstates/sha256/500"))
}

func TestCreateUploadFileAndMoveToCache(t *testing.T) {
	c, s := GetTestFileStore()
	defer cleanupTestFileStore(c)

	err := s.CreateUploadFile("test_file.txt", 100)
	assert.Nil(t, err)
	err = s.SetUploadFileHashState("test_file.txt", []byte{uint8(0), uint8(1)}, "sha256", "500")
	assert.Nil(t, err)
	b, err := s.GetUploadFileHashState("test_file.txt", "sha256", "500")
	assert.Nil(t, err)
	assert.Equal(t, uint8(0), b[0])
	assert.Equal(t, uint8(1), b[1])
	err = s.SetUploadFileStartedAt("test_file.txt", []byte{uint8(2), uint8(3)})
	assert.Nil(t, err)
	b, err = s.GetUploadFileStartedAt("test_file.txt")
	assert.Nil(t, err)
	assert.Equal(t, uint8(2), b[0])
	assert.Equal(t, uint8(3), b[1])
	_, err = os.Stat(path.Join(c.UploadDir, "test_file.txt"))
	assert.Nil(t, err)

	err = s.MoveUploadFileToCache("test_file.txt", "test_file_cache.txt")
	assert.Nil(t, err)
	_, err = os.Stat(path.Join(c.UploadDir, "test_file.txt"))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(path.Join(c.CacheDir, "test_file_cache.txt"))
	assert.Nil(t, err)
}

func TestDownloadAndDeleteFiles(t *testing.T) {
	c, s := GetTestFileStore()
	defer cleanupTestFileStore(c)

	var waitGroup sync.WaitGroup

	for i := 0; i < 100; i++ {
		waitGroup.Add(1)

		testFileName := fmt.Sprintf("test_%d", i)
		go func() {
			err := s.CreateDownloadFile(testFileName, 1)
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
		_, err := os.Stat(path.Join(c.TrashDir, testFileName))
		assert.True(t, os.IsNotExist(err))
	}
}
