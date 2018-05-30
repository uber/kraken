package store

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/core"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestCreateUploadFileAndMoveToCache(t *testing.T) {
	require := require.New(t)

	config, cleanup := ConfigFixture()
	defer cleanup()

	s, err := NewLocalFileStore(config, tally.NewTestScope("", nil))
	require.NoError(err)

	filename := "test_file.txt"

	require.NoError(s.CreateUploadFile(filename, 100))
	require.NoError(s.MoveUploadFileToCache("test_file.txt", "test_file_cache.txt"))

	_, err = os.Stat(path.Join(s.Config().UploadDir, "test_file.txt"))
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(s.Config().CacheDir, "te", "st", "test_file_cache.txt"))
	require.NoError(err)
}

func TestDownloadAndDeleteFiles(t *testing.T) {
	require := require.New(t)

	config, cleanup := ConfigFixture()
	defer cleanup()

	s, err := NewLocalFileStore(config, tally.NewTestScope("", nil))
	require.NoError(err)

	var names []string
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		name := core.DigestFixture().Hex()
		names = append(names, name)
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(s.CreateDownloadFile(name, 1))
			require.NoError(s.MoveDownloadFileToCache(name))
			require.NoError(s.DeleteDownloadOrCacheFile(name))
		}()
	}
	wg.Wait()

	for _, name := range names {
		_, err := s.GetCacheFileStat(name)
		require.True(os.IsNotExist(err))
	}
}

func TestCreateCacheFile(t *testing.T) {
	require := require.New(t)

	s, cleanup := LocalFileStoreFixture()
	defer cleanup()

	s1 := "buffer"
	computedDigest, err := core.NewDigester().FromBytes([]byte(s1))
	require.NoError(err)
	r1 := strings.NewReader(s1)

	err = s.CreateCacheFile(computedDigest.Hex(), r1)
	require.NoError(err)
	r2, err := s.GetCacheFileReader(computedDigest.Hex())
	require.NoError(err)
	b2, err := ioutil.ReadAll(r2)
	require.Equal(s1, string(b2))
}
