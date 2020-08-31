// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
)

func TestCAStoreInitVolumes(t *testing.T) {
	require := require.New(t)

	config, cleanup := CAStoreConfigFixture()
	defer cleanup()

	volume1, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume1)

	volume2, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume2)

	volume3, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume3)

	config.Volumes = []Volume{
		{Location: volume1, Weight: 100},
		{Location: volume2, Weight: 100},
		{Location: volume3, Weight: 100},
	}

	_, err = NewCAStore(config, tally.NoopScope)
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
	require.True(float32(n1)/256 > float32(0.25), "%d/256 should be >0.25", n1)
	require.True(float32(n2)/256 > float32(0.25), "%d/256 should be >0.25", n2)
	require.True(float32(n3)/256 > float32(0.25), "%d/256 should be >0.25", n3)
}

func TestCAStoreInitVolumesAfterChangingVolumes(t *testing.T) {
	require := require.New(t)

	config, cleanup := CAStoreConfigFixture()
	defer cleanup()

	volume1, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume1)

	volume2, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume2)

	volume3, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume3)

	config.Volumes = []Volume{
		{Location: volume1, Weight: 100},
		{Location: volume2, Weight: 100},
		{Location: volume3, Weight: 100},
	}

	_, err = NewCAStore(config, tally.NoopScope)
	require.NoError(err)

	// Add one more volume, recreate file store.

	volume4, err := ioutil.TempDir("/tmp", "volume")
	require.NoError(err)
	defer os.RemoveAll(volume3)

	config.Volumes = append(config.Volumes, Volume{Location: volume4, Weight: 100})

	_, err = NewCAStore(config, tally.NoopScope)
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
	require.True(float32(n1)/256 > float32(0.15))
	require.True(float32(n2)/256 > float32(0.15))
	require.True(float32(n3)/256 > float32(0.15))
	require.True(float32(n4)/256 > float32(0.15))
}

func TestCAStoreCreateUploadFileAndMoveToCache(t *testing.T) {
	require := require.New(t)

	config, cleanup := CAStoreConfigFixture()
	defer cleanup()

	s, err := NewCAStore(config, tally.NoopScope)
	require.NoError(err)

	src := core.DigestFixture().Hex()

	require.NoError(s.CreateUploadFile(src, 100))
	_, err = os.Stat(path.Join(config.UploadDir, src))
	require.NoError(err)

	f, err := s.uploadStore.newFileOp().GetFileReader(src)
	require.NoError(err)
	defer f.Close()
	digester := core.NewDigester()
	digest, err := digester.FromReader(f)
	require.NoError(err)
	dst := digest.Hex()

	err = s.MoveUploadFileToCache(src, dst)
	require.NoError(err)
	_, err = os.Stat(path.Join(config.UploadDir, src[:2], src[2:4], src))
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(config.CacheDir, dst[:2], dst[2:4], dst))
	require.NoError(err)
}

func TestCAStoreCreateUploadFileAndMoveToCacheFailure(t *testing.T) {
	require := require.New(t)

	config, cleanup := CAStoreConfigFixture()
	defer cleanup()

	s, err := NewCAStore(config, tally.NoopScope)
	require.NoError(err)

	src := core.DigestFixture().Hex()

	require.NoError(s.CreateUploadFile(src, 100))
	_, err = os.Stat(path.Join(config.UploadDir, src))
	require.NoError(err)

	f, err := s.uploadStore.newFileOp().GetFileReader(src)
	require.NoError(err)
	defer f.Close()
	digester := core.NewDigester()
	digest, err := digester.FromReader(f)
	require.NoError(err)

	dst := core.DigestFixture().Hex()
	err = s.MoveUploadFileToCache(src, dst)
	require.EqualError(err, fmt.Sprintf("verify digest: computed digest sha256:%s doesn't match expected value sha256:%s", digest.Hex(), dst))
	_, err = os.Stat(path.Join(config.UploadDir, src[:2], src[2:4], src))
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(config.CacheDir, dst[:2], dst[2:4], dst))
	require.True(os.IsNotExist(err))
}

func TestCAStoreCreateCacheFile(t *testing.T) {
	require := require.New(t)

	s, cleanup := CAStoreFixture()
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
