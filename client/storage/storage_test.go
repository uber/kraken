package storage

import (
	"testing"

	cache "code.uber.internal/infra/dockermover/storage"
	"code.uber.internal/infra/kraken/configuration"

	"os"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"
)

func TestNewManager(t *testing.T) {
	assert := require.New(t)
	cp := configuration.GetConfigFilePath("test.yaml")
	c := configuration.NewConfig(cp)
	os.RemoveAll(c.DownloadDir)
	os.MkdirAll(c.DownloadDir, 0755)
	os.RemoveAll(c.CacheDir)
	os.MkdirAll(c.CacheDir, 0755)
	l, _ := cache.NewFileCacheMap(c.CacheMapSize, c.CacheSize)
	m, err := NewManager(c, l)
	assert.Nil(err)
	m.LoadFromDisk(nil)
	assert.Empty(m.opened)
	assert.Nil(m.Close())
}

func TestLoadFromDisk(t *testing.T) {
	assert := require.New(t)
	cp := configuration.GetConfigFilePath("test.yaml")
	c := configuration.NewConfig(cp)
	os.RemoveAll(c.DownloadDir)
	os.MkdirAll(c.DownloadDir, 0755)
	os.RemoveAll(c.CacheDir)
	os.MkdirAll(c.CacheDir, 0755)
	l, _ := cache.NewFileCacheMap(c.CacheMapSize, c.CacheSize)
	// test torrent1
	f1, err := os.Create(c.DownloadDir + "torrent1")
	assert.Nil(err)
	f1.Close()
	f1s, err := os.Create(c.DownloadDir + "torrent1" + statusSuffix)
	assert.Nil(err)
	status := []byte{uint8(0), uint8(1), uint8(2)}
	_, err = f1s.Write(status)
	assert.Nil(err)
	f1s.Close()

	// test cache1
	f2, err := os.Create(c.CacheDir + "76cache1")
	assert.Nil(err)
	f2.Close()

	m, err := NewManager(c, l)
	assert.Nil(err)
	m.LoadFromDisk(nil)
	//assert.Equal(1, len(m.opened))
	ls := m.opened["torrent1"]
	assert.Equal(3, len(ls.pieces))
	assert.Equal(clean, ls.pieces[0].status)
	assert.Equal(dirty, ls.pieces[1].status)
	assert.Equal(done, ls.pieces[2].status)
	fp, ok := m.lru.Get("76cache1", nil)
	assert.True(ok)
	assert.Equal(c.CacheDir+"76cache1", fp)
}

func TestOpenTorrent(t *testing.T) {
	assert := require.New(t)
	cp := configuration.GetConfigFilePath("test.yaml")
	c := configuration.NewConfig(cp)
	os.RemoveAll(c.DownloadDir)
	os.MkdirAll(c.DownloadDir, 0755)
	l, _ := cache.NewFileCacheMap(c.CacheMapSize, c.CacheSize)
	// test torrent1
	f1, err := os.Create(c.DownloadDir + "torrent1")
	assert.Nil(err)
	f1.Close()
	f1s, err := os.Create(c.DownloadDir + "torrent1" + statusSuffix)
	assert.Nil(err)
	status := []byte{uint8(0), uint8(1), uint8(2)}
	_, err = f1s.Write(status)
	assert.Nil(err)
	f1s.Close()

	m, err := NewManager(c, l)
	assert.Nil(err)
	m.LoadFromDisk(nil)
	//assert.Equal(1, len(m.opened))
	ls, err := m.OpenTorrent(&metainfo.Info{Name: "torrent1"}, metainfo.Hash([20]byte{}))
	assert.Nil(err)
	assert.Equal(3, ls.(*LayerStore).numPieces())
	assert.Equal(clean, ls.(*LayerStore).pieces[0].status)
	assert.Equal(dirty, ls.(*LayerStore).pieces[1].status)
	assert.Equal(done, ls.(*LayerStore).pieces[2].status)

	ls1, err := m.OpenTorrent(&metainfo.Info{Name: "torrent2", Length: int64(1), Pieces: make([]byte, 60)}, metainfo.Hash([20]byte{}))
	assert.Nil(err)
	//assert.Equal(2, len(m.opened))
	assert.Equal(3, ls1.(*LayerStore).numPieces())
	assert.Equal(clean, ls1.(*LayerStore).pieces[0].status)
	assert.Equal(clean, ls1.(*LayerStore).pieces[1].status)
	assert.Equal(clean, ls1.(*LayerStore).pieces[2].status)

	// open again
	ls1, err = m.OpenTorrent(&metainfo.Info{Name: "torrent2", Length: int64(1), Pieces: make([]byte, 60)}, metainfo.Hash([20]byte{}))
	assert.Nil(err)
	//assert.Equal(2, len(m.opened))
	assert.Equal(3, ls1.(*LayerStore).numPieces())
	assert.Equal(clean, ls1.(*LayerStore).pieces[0].status)
	assert.Equal(clean, ls1.(*LayerStore).pieces[1].status)
	assert.Equal(clean, ls1.(*LayerStore).pieces[2].status)
}
