package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
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

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		testFileName := fmt.Sprintf("test_%d", i)
		go func() {
			err := s.CreateDownloadFile(testFileName, 1)
			require.Nil(err)
			err = s.MoveDownloadFileToCache(testFileName)
			require.Nil(err)
			err = s.MoveCacheFileToTrash(testFileName)
			require.Nil(err)

			wg.Done()
		}()
	}

	wg.Wait()
	err := s.DeleteAllTrashFiles()
	require.Nil(err)

	for i := 0; i < 100; i++ {
		testFileName := fmt.Sprintf("test_%d", i)
		_, err := os.Stat(path.Join(s.Config().TrashDir, testFileName))
		require.True(os.IsNotExist(err))
	}
}

func TestCreateCacheFile(t *testing.T) {
	require := require.New(t)

	s, cleanup := LocalFileStoreFixture()
	defer cleanup()

	s1 := "buffer"
	computedDigest, err := image.NewDigester().FromBytes([]byte(s1))
	require.NoError(err)
	r1 := strings.NewReader(s1)

	err = s.CreateCacheFile(computedDigest.Hex(), r1)
	require.NoError(err)
	r2, err := s.GetCacheFileReader(computedDigest.Hex())
	require.NoError(err)
	b2, err := ioutil.ReadAll(r2)
	require.Equal(s1, string(b2))
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
	shardsMap := make(map[string]string)
	for i := 0; i < 100; i++ {
		name := randutil.Hex(32)
		if _, ok := shardsMap[name[:4]]; ok {
			// Avoid duplicated names
			continue
		}
		cacheFiles = append(cacheFiles, name)
		shardsMap[name[:4]] = name
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
