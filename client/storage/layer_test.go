package storage

import (
	"os"
	"testing"

	cache "code.uber.internal/infra/dockermover/storage"
	"code.uber.internal/infra/kraken/configuration"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"
)

func getManager() (*configuration.Config, *Manager) {
	cp := configuration.GetConfigFilePath("test.yaml")
	c := configuration.NewConfig(cp)
	os.RemoveAll(c.DownloadDir)
	os.MkdirAll(c.DownloadDir, 0755)
	os.RemoveAll(c.CacheDir)
	os.MkdirAll(c.CacheDir, 0755)
	l, _ := cache.NewFileCacheMap(c.CacheMapSize, c.CacheSize)
	m, _ := NewManager(c, l)
	return c, m
}

func TestNewLayerStore(t *testing.T) {
	assert := require.New(t)
	c, m := getManager()
	ls := NewLayerStore(m, "layer1")
	assert.Equal("layer1", ls.name)
	assert.Equal(0, len(ls.pieces))
	assert.Equal(0, ls.numPieces())
	assert.Equal("layer1", GetLayerKey(ls.name))
	assert.Equal(c.DownloadDir+"layer1", ls.downloadPath())
	assert.Equal(c.CacheDir+"layer1", ls.cachePath())
	assert.Equal(c.DownloadDir+"layer1-status", ls.pieceStatusPath())
}

func TestIsDownloading(t *testing.T) {
	assert := require.New(t)
	_, m := getManager()
	ls := NewLayerStore(m, "layer1")
	assert.False(ls.IsDownloading())
}

func TestIsDownloaded(t *testing.T) {
	assert := require.New(t)
	_, m := getManager()
	ls := NewLayerStore(m, "layer1")
	_, downloaded := ls.IsDownloaded()
	assert.False(downloaded)
}

func TestTryCacheLayer(t *testing.T) {
	assert := require.New(t)
	_, m := getManager()
	ls := NewLayerStore(m, "00")
	err := ls.CreateEmptyLayerFile(1, 1)
	assert.Nil(err)
	assert.Equal(1, ls.numPieces())
	assert.Equal("Download is not completed yet. Unable to cache layer file 00", ls.TryCacheLayer().Error())

	ps := ls.pieces[0]
	ok, err := ps.compareAndSwapStatus(ps.ls.pieceStatusPath(), clean, dirty)
	assert.Nil(err)
	assert.True(ok)
	assert.Equal("Download is not completed yet. Unable to cache layer file 00", ls.TryCacheLayer().Error())

	ok, err = ps.compareAndSwapStatus(ps.ls.pieceStatusPath(), dirty, done)
	assert.Nil(err)
	assert.True(ok)
	f, _ := os.OpenFile(ps.ls.pieceStatusPath(), os.O_RDWR, perm)
	f.Write([]byte{done})
	f.Close()
	err = ls.TryCacheLayer()
	assert.Nil(err)
	_, ok = ls.m.lru.Get("00", nil)
	assert.True(ok)
}

func TestPiece(t *testing.T) {
	assert := require.New(t)
	info := metainfo.Info{
		Name:   "torrent",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	pinfo := info.Piece(0)
	_, m := getManager()
	ls, _ := m.OpenTorrent(&info, metainfo.Hash([20]byte{}))
	ps := ls.Piece(pinfo)
	assert.Equal(0, ps.(*PieceStore).index)
	assert.Equal(ls, ps.(*PieceStore).ls)
	assert.Equal(clean, ps.(*PieceStore).status)
	ok, err := ps.(*PieceStore).compareAndSwapStatus(ps.(*PieceStore).ls.pieceStatusPath(), clean, dirty)
	assert.Nil(err)
	assert.True(ok)
	assert.Equal(dirty, ps.(*PieceStore).status)

	pinfo2 := info.Piece(1)
	ps2 := ls.Piece(pinfo2)
	assert.Nil(ps2)
}

func TestLayerClose(t *testing.T) {
	assert := require.New(t)
	info := metainfo.Info{
		Name:   "01",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	_, m := getManager()
	ls, _ := m.OpenTorrent(&info, metainfo.Hash([20]byte{}))

	ps := ls.(*LayerStore).pieces[0]
	ok, err := ps.compareAndSwapStatus(ps.ls.pieceStatusPath(), clean, done)
	assert.Nil(err)
	assert.True(ok)

	assert.Equal(1, len(m.opened))
	ls.Close()
	_, ok = ls.(*LayerStore).m.lru.Get("01", nil)
	assert.True(ok)
	assert.Empty(m.opened)
}
