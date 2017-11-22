package store

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/utils/randutil"

	"github.com/stretchr/testify/require"
)

func TestFileHashStates(t *testing.T) {
	require := require.New(t)

	s, cleanup := LocalFileStoreWithRefcountFixture()
	defer cleanup()

	s.CreateUploadFile("test_file.txt", 100)
	err := s.SetUploadFileHashState("test_file.txt", []byte{uint8(0), uint8(1)}, "sha256", "500")
	require.Nil(err)
	b, err := s.GetUploadFileHashState("test_file.txt", "sha256", "500")
	require.Nil(err)
	require.Equal(uint8(0), b[0])
	require.Equal(uint8(1), b[1])

	l, err := s.ListUploadFileHashStatePaths("test_file.txt")
	require.Nil(err)
	require.Equal(len(l), 1)
	require.True(strings.HasSuffix(l[0], "/hashstates/sha256/500"))
}

func TestCreateUploadFileAndMoveToCache(t *testing.T) {
	require := require.New(t)

	s, cleanup := LocalFileStoreWithRefcountFixture()
	defer cleanup()

	err := s.CreateUploadFile("test_file.txt", 100)
	require.Nil(err)
	err = s.SetUploadFileHashState("test_file.txt", []byte{uint8(0), uint8(1)}, "sha256", "500")
	require.Nil(err)
	b, err := s.GetUploadFileHashState("test_file.txt", "sha256", "500")
	require.Nil(err)
	require.Equal(uint8(0), b[0])
	require.Equal(uint8(1), b[1])
	err = s.SetUploadFileStartedAt("test_file.txt", []byte{uint8(2), uint8(3)})
	require.Nil(err)
	b, err = s.GetUploadFileStartedAt("test_file.txt")
	require.Nil(err)
	require.Equal(uint8(2), b[0])
	require.Equal(uint8(3), b[1])
	_, err = os.Stat(path.Join(s.Config().UploadDir, "test_file.txt"))
	require.Nil(err)

	err = s.MoveUploadFileToCache("test_file.txt", "test_file_cache.txt")
	require.Nil(err)
	_, err = os.Stat(path.Join(s.Config().UploadDir, "test_file.txt"))
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(s.Config().CacheDir, "te", "st", "test_file_cache.txt"))
	require.Nil(err)
}

func TestDownloadAndDeleteFiles(t *testing.T) {
	require := require.New(t)

	s, cleanup := LocalFileStoreWithRefcountFixture()
	defer cleanup()

	var waitGroup sync.WaitGroup

	for i := 0; i < 100; i++ {
		waitGroup.Add(1)

		testFileName := fmt.Sprintf("test_%d", i)
		go func() {
			err := s.CreateDownloadFile(testFileName, 1)
			require.Nil(err)
			err = s.MoveDownloadFileToCache(testFileName)
			require.Nil(err)
			err = s.MoveCacheFileToTrash(testFileName)
			require.Nil(err)

			waitGroup.Done()
		}()
	}

	waitGroup.Wait()
	err := s.DeleteAllTrashFiles()
	require.Nil(err)

	for i := 0; i < 100; i++ {
		testFileName := fmt.Sprintf("test_%d", i)
		_, err := os.Stat(path.Join(s.Config().TrashDir, testFileName))
		require.True(os.IsNotExist(err))
	}
}

func TestTrashDeletionCronDeletesFiles(t *testing.T) {
	require := require.New(t)

	interval := time.Second

	s, cleanup := LocalFileStoreWithTrashDeletionFixture(interval)
	defer cleanup()

	f := "test_file.txt"
	require.NoError(s.CreateDownloadFile(f, 1))
	require.NoError(s.MoveDownloadOrCacheFileToTrash(f))

	time.Sleep(interval + 250*time.Millisecond)

	_, err := os.Stat(path.Join(s.Config().TrashDir, f))
	require.True(os.IsNotExist(err))
}

func TestListPopulatedShardIDs(t *testing.T) {
	require := require.New(t)

	s, cleanup := LocalFileStoreFixture()
	defer cleanup()

	var cacheFiles []string
	cacheFileMap := make(map[string]string)
	for i := 0; i < 100; i++ {
		name := randutil.Hex(32)
		if _, ok := cacheFileMap[name]; ok {
			// Avoid duplicated names
			continue
		}
		cacheFiles = append(cacheFiles, name)
		cacheFileMap[name] = name
		require.NoError(s.CreateUploadFile(name, 1))
		require.NoError(s.MoveUploadFileToCache(name, name))
		if i >= 50 {
			require.NoError(s.MoveCacheFileToTrash(name))
		}
	}
	shards, err := s.ListPopulatedShardIDs()
	require.NoError(err)

	for i, name := range cacheFiles {
		shard := name[:4]
		if i < 50 {
			require.Contains(shards, shard)
		} else {
			require.NotContains(shards, shard)
		}
	}
}
