package store

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"code.uber.internal/infra/kraken/core"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func TestOriginFileStoreInitDirectories(t *testing.T) {
	require := require.New(t)

	s, c := OriginFileStoreFixture(clock.New())
	defer c()

	volume1, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume1)

	volume2, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume2)

	volume3, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume3)

	volume4, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume4)

	// Update config, add volumes.
	config := s.Config()
	config.Volumes = []Volume{
		{Location: volume1, Weight: 100},
		{Location: volume2, Weight: 100},
		{Location: volume3, Weight: 100},
	}
	_, err = NewOriginFileStore(config, clock.NewMock())
	require.NoError(err)

	v1Files, err := ioutil.ReadDir(path.Join(volume1, path.Base(config.CacheDir)))
	require.NoError(err)
	v2Files, err := ioutil.ReadDir(path.Join(volume2, path.Base(config.CacheDir)))
	require.NoError(err)
	v3Files, err := ioutil.ReadDir(path.Join(volume3, path.Base(config.CacheDir)))
	require.NoError(err)
	n1 := len(v1Files)
	n2 := len(v2Files)
	n3 := len(v3Files)

	// There should be 256 symlinks total, evenly distributed across the volumes.
	require.Equal(256, (n1 + n2 + n3))
	require.True(float32(n1)/255 > float32(0.25))
	require.True(float32(n2)/255 > float32(0.25))
	require.True(float32(n3)/255 > float32(0.25))
}

func TestOriginFileStoreInitDirectoriesAfterChangingVolumes(t *testing.T) {
	require := require.New(t)

	s, c := OriginFileStoreFixture(clock.New())
	defer c()

	volume1, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume1)

	volume2, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume2)

	volume3, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume3)

	volume4, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume4)

	// Update config, add volumes, create file store.
	config := s.Config()
	config.Volumes = []Volume{
		{Location: volume1, Weight: 100},
		{Location: volume2, Weight: 100},
		{Location: volume3, Weight: 100},
	}
	_, err = NewOriginFileStore(config, clock.NewMock())
	require.NoError(err)

	// Add one more volume, recreate file store.
	config.Volumes = append(config.Volumes, Volume{Location: volume4, Weight: 100})
	_, err = NewOriginFileStore(config, clock.NewMock())
	require.NoError(err)

	var n1, n2, n3, n4 int
	links, err := ioutil.ReadDir(config.CacheDir)
	require.NoError(err)
	for _, link := range links {
		source, err := os.Readlink(path.Join(config.CacheDir, link.Name()))
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

	// Symlinks should be recreated.
	require.Equal(256, (n1 + n2 + n3 + n4))
	require.True(float32(n1)/255 > float32(0.15))
	require.True(float32(n2)/255 > float32(0.15))
	require.True(float32(n3)/255 > float32(0.15))
	require.True(float32(n4)/255 > float32(0.15))
}

func TestOriginFileStoreCreateUploadFileAndMoveToCache(t *testing.T) {
	require := require.New(t)

	s, cleanup := OriginFileStoreFixture(clock.New())
	defer cleanup()

	testFileName := "test_file.txt"

	err := s.CreateUploadFile(testFileName, 100)
	require.NoError(err)
	_, err = os.Stat(path.Join(s.Config().UploadDir, testFileName))
	require.NoError(err)

	err = s.MoveUploadFileToCache(testFileName, "test_file_cache.txt")
	require.NoError(err)
	_, err = os.Stat(path.Join(s.Config().UploadDir, testFileName))
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(s.Config().CacheDir, "te", "st", "test_file_cache.txt"))
	require.NoError(err)
}

func TestOriginFileStoreCreateCacheFile(t *testing.T) {
	require := require.New(t)

	s, cleanup := OriginFileStoreFixture(clock.New())
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

func TestOriginFileStoreFileHashStates(t *testing.T) {
	require := require.New(t)

	s, cleanup := OriginFileStoreFixture(clock.New())
	defer cleanup()

	err := s.CreateUploadFile("test_file.txt", 100)
	require.NoError(err)
	err = s.SetUploadFileHashState("test_file.txt", []byte{uint8(0), uint8(1)}, "sha256", "500")
	require.NoError(err)
	b, err := s.GetUploadFileHashState("test_file.txt", "sha256", "500")
	require.NoError(err)
	require.Equal(uint8(0), b[0])
	require.Equal(uint8(1), b[1])

	l, err := s.ListUploadFileHashStatePaths("test_file.txt")
	require.NoError(err)
	require.Equal(len(l), 1)
	require.True(strings.HasSuffix(l[0], "/hashstates/sha256/500"))
}

func TestOriginFileStoreFileStartedAt(t *testing.T) {
	require := require.New(t)

	s, cleanup := OriginFileStoreFixture(clock.New())
	defer cleanup()

	err := s.CreateUploadFile("test_file.txt", 100)
	require.NoError(err)
	err = s.SetUploadFileStartedAt("test_file.txt", []byte{uint8(2), uint8(3)})
	require.NoError(err)
	b, err := s.GetUploadFileStartedAt("test_file.txt")
	require.NoError(err)
	require.Equal(uint8(2), b[0])
	require.Equal(uint8(3), b[1])
	_, err = os.Stat(path.Join(s.Config().UploadDir, "test_file.txt"))
	require.NoError(err)
}

func TestOriginStoreListPopulatedShardIDs(t *testing.T) {
	require := require.New(t)

	s, cleanup := OriginFileStoreFixture(clock.New())
	defer cleanup()

	var cacheFiles []string
	shardsMap := make(map[string]string)
	for i := 0; i < 100; i++ {
		name := core.DigestFixture().Hex()
		if _, ok := shardsMap[name[:4]]; ok {
			// Avoid duplicated names
			continue
		}
		cacheFiles = append(cacheFiles, name)
		shardsMap[name[:4]] = name
		require.NoError(s.CreateUploadFile(name, 1))
		require.NoError(s.MoveUploadFileToCache(name, name))
		if i >= 50 {
			require.NoError(s.DeleteCacheFile(name))
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
