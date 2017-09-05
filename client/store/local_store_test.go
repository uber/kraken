package store

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileHashStates(t *testing.T) {
	s, cleanup := LocalStoreWithRefcountFixture()
	defer cleanup()

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

func helperCreateUploadAndMoveToCache(t *testing.T, s *LocalStore, ufn string, cfn string) {
	err := s.CreateUploadFile(ufn, 100)
	assert.Nil(t, err)
	err = s.SetUploadFileHashState(ufn, []byte{uint8(0), uint8(1)}, "sha256", "500")
	assert.Nil(t, err)
	b, err := s.GetUploadFileHashState(ufn, "sha256", "500")
	assert.Nil(t, err)
	assert.Equal(t, uint8(0), b[0])
	assert.Equal(t, uint8(1), b[1])
	err = s.SetUploadFileStartedAt(ufn, []byte{uint8(2), uint8(3)})
	assert.Nil(t, err)
	b, err = s.GetUploadFileStartedAt(ufn)
	assert.Nil(t, err)
	assert.Equal(t, uint8(2), b[0])
	assert.Equal(t, uint8(3), b[1])
	_, err = os.Stat(path.Join(s.Config().UploadDir, ufn))
	assert.Nil(t, err)

	err = s.MoveUploadFileToCache(ufn, cfn)
	assert.Nil(t, err)
	_, err = os.Stat(path.Join(s.Config().UploadDir, ufn))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(path.Join(s.Config().CacheDir, cfn))
	assert.Nil(t, err)
}

func TestCreateUploadFileAndMoveToCache(t *testing.T) {
	s, cleanup := LocalStoreWithRefcountFixture()
	defer cleanup()

	helperCreateUploadAndMoveToCache(t, s, "test_file.txt", "test_file_cache.txt")
}

func TestDownloadAndDeleteFiles(t *testing.T) {
	s, cleanup := LocalStoreWithRefcountFixture()
	defer cleanup()

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
		_, err := os.Stat(path.Join(s.Config().TrashDir, testFileName))
		assert.True(t, os.IsNotExist(err))
	}
}

func TestTrashDeletionCronDeletesFiles(t *testing.T) {
	require := require.New(t)

	interval := time.Second

	s, cleanup := LocalStoreWithTrashDeletionFixture(interval)
	defer cleanup()

	f := "test_file.txt"
	require.NoError(s.CreateDownloadFile(f, 1))
	require.NoError(s.MoveDownloadOrCacheFileToTrash(f))

	time.Sleep(interval + 250*time.Millisecond)

	_, err := os.Stat(path.Join(s.Config().TrashDir, f))
	require.True(os.IsNotExist(err))
}

func helperDigestsToStrings(digests []*image.Digest) []string {
	var sd []string
	for _, d := range digests {
		sd = append(sd, d.Hex())
	}

	return sd
}

func TestListDigests(t *testing.T) {

	s, cleanup := LocalStoreFixture()
	defer cleanup()

	cfn := "1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9"
	helperCreateUploadAndMoveToCache(t, s, "test_file.txt", cfn)

	digests, err := s.ListDigests(cfn[:4])

	assert.NoError(t, err)
	assert.Equal(t, helperDigestsToStrings(digests),
		[]string{"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9"})
}
