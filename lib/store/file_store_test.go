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

	"code.uber.internal/infra/kraken/utils/randutil"

	"github.com/stretchr/testify/require"
)

func TestInitDirectories(t *testing.T) {
	require := require.New(t)

	s, cleanup := LocalFileStoreWithRefcountFixture()
	defer cleanup()

	volume1, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	volume2, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	volume3, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	volume4, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume1)
	defer os.RemoveAll(volume2)
	defer os.RemoveAll(volume3)
	defer os.RemoveAll(volume4)

	// Update config, add volumes
	config := s.Config()
	config.Volumes = []Volume{
		{Location: volume1, Weight: 100},
		{Location: volume2, Weight: 100},
		{Location: volume3, Weight: 100},
	}
	_, err = NewLocalFileStore(&config, false)
	require.NoError(err)

	for _, stateDir := range []string{
		path.Base(config.DownloadDir),
		path.Base(config.CacheDir),
		path.Base(config.TrashDir),
	} {
		v1Files, err := ioutil.ReadDir(path.Join(volume1, stateDir))
		require.NoError(err)
		v2Files, err := ioutil.ReadDir(path.Join(volume2, stateDir))
		require.NoError(err)
		v3Files, err := ioutil.ReadDir(path.Join(volume3, stateDir))
		require.NoError(err)
		n1 := len(v1Files)
		n2 := len(v2Files)
		n3 := len(v3Files)
		// There should be 256 symlinks total, evenly ditributed across the volumes.
		require.Equal((n1 + n2 + n3), 256)
		require.True(float32(n1)/255 > float32(0.25))
		require.True(float32(n2)/255 > float32(0.25))
		require.True(float32(n3)/255 > float32(0.25))
	}
}

func TestInitDirectoriesAfterChangingVolumes(t *testing.T) {
	require := require.New(t)

	s, cleanup := LocalFileStoreWithRefcountFixture()
	defer cleanup()

	volume1, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	volume2, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	volume3, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	volume4, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume1)
	defer os.RemoveAll(volume2)
	defer os.RemoveAll(volume3)
	defer os.RemoveAll(volume4)

	// Update config, add volumes, create file store.
	config := s.Config()
	config.Volumes = []Volume{
		{Location: volume1, Weight: 100},
		{Location: volume2, Weight: 100},
		{Location: volume3, Weight: 100},
	}
	_, err = NewLocalFileStore(&config, false)
	require.NoError(err)

	// Add one more volume, recreate file store.
	config.Volumes = append(config.Volumes, Volume{Location: volume4, Weight: 100})
	_, err = NewLocalFileStore(&config, false)
	require.NoError(err)
	for _, stateDir := range []string{
		config.DownloadDir,
		config.CacheDir,
		config.TrashDir,
	} {
		var n1, n2, n3, n4 int
		links, err := ioutil.ReadDir(stateDir)
		require.NoError(err)
		for _, link := range links {
			source, err := os.Readlink(path.Join(stateDir, link.Name()))
			require.NoError(err)
			if strings.HasPrefix(source, volume1) {
				n1++
			}
			if strings.HasPrefix(source, volume2) {
				n2++
			}
			if strings.HasPrefix(source, volume3) {
				n3++
			}
			if strings.HasPrefix(source, volume4) {
				n4++
			}
		}
		// Symlinks should be recreated
		require.Equal((n1 + n2 + n3 + n4), 256)
		require.True(float32(n1)/255 > float32(0.15))
		require.True(float32(n2)/255 > float32(0.15))
		require.True(float32(n3)/255 > float32(0.15))
		require.True(float32(n4)/255 > float32(0.15))
	}
}

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
