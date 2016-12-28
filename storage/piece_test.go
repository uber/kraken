package storage

import (
	"testing"

	"io/ioutil"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"
)

func TestCompareAndSwapStatus(t *testing.T) {
	assert := require.New(t)
	info := metainfo.Info{
		Name:   "01",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	_, m := getManager()
	ls, _ := m.OpenTorrent(&info, metainfo.Hash([20]byte{}))
	ps := ls.(*LayerStore).pieces[0]

	// currstatus = clean, newstatus = done
	ok, err := ps.compareAndSwapStatus(ps.ls.pieceStatusPath(), clean, done)
	assert.Nil(err)
	assert.True(ok)

	// currstatus = done, newstatus = done
	ok, err = ps.compareAndSwapStatus(ps.ls.pieceStatusPath(), dirty, done)
	assert.Nil(err)
	assert.False(ok)

	// currstatus = done, newstatus = clean
	ok, err = ps.compareAndSwapStatus(ps.ls.pieceStatusPath(), dirty, clean)
	assert.Equal("Current status not matched. Expected: 1. Actual: 2", err.Error())
	assert.False(ok)

	// currstatus = done, newstatus = clean
	ok, err = ps.compareAndSwapStatus(ps.ls.pieceStatusPath(), dc, clean)
	assert.Nil(err)
	assert.True(ok)

}

func TestWriteAt(t *testing.T) {
	assert := require.New(t)
	info := metainfo.Info{
		Name:   "02",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	_, m := getManager()
	ls, _ := m.OpenTorrent(&info, metainfo.Hash([20]byte{}))
	ps := ls.(*LayerStore).pieces[0]

	assert.Equal(0, ps.index)
	n, err := ps.WriteAt([]byte{uint8(7)}, 0)
	assert.Nil(err)
	assert.Equal(1, n)
	assert.Equal(clean, ps.status)
	data, _ := ioutil.ReadFile(ls.(*LayerStore).downloadPath())
	assert.Equal(uint8(7), data[0])
	statusOnDisk, err := ioutil.ReadFile(ls.(*LayerStore).pieceStatusPath())
	assert.Nil(err)
	assert.Equal(clean, statusOnDisk[0])
}

func TestReadAt(t *testing.T) {
	// downloading
	assert := require.New(t)
	info := metainfo.Info{
		Name:   "03",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	_, m := getManager()
	ls, _ := m.OpenTorrent(&info, metainfo.Hash([20]byte{}))
	ps := ls.(*LayerStore).pieces[0]

	ps.WriteAt([]byte{uint8(7)}, 0)
	bu := make([]byte, 1)
	ps.ReadAt(bu, 0)
	assert.Equal(uint8(7), bu[0])
	downloading, _ := ps.ls.IsDownloading()
	assert.True(downloading)

	// cached
	ps.WriteAt([]byte{uint8(9)}, 0)
	ioutil.WriteFile(ps.ls.pieceStatusPath(), []byte{done}, perm)
	ps.ls.TryCacheLayer()
	_, downloaded := ps.ls.IsDownloaded()
	assert.True(downloaded)
	_, ok := ls.(*LayerStore).m.lru.Get("03", nil)
	assert.True(ok)
	ps.ReadAt(bu, 0)
	assert.Equal(uint8(9), bu[0])
}

func TestMarkComplete(t *testing.T) {
	assert := require.New(t)
	info := metainfo.Info{
		Name:   "04",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	_, m := getManager()
	ls, _ := m.OpenTorrent(&info, metainfo.Hash([20]byte{}))
	ps := ls.(*LayerStore).pieces[0]

	assert.Nil(ps.MarkComplete())
	ioutil.WriteFile(ps.ls.pieceStatusPath(), []byte{dirty}, perm)
	err := ps.MarkComplete()
	assert.Nil(err)
}

func TestMarkNotComplete(t *testing.T) {
	assert := require.New(t)
	info := metainfo.Info{
		Name:   "05",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	_, m := getManager()
	ls, _ := m.OpenTorrent(&info, metainfo.Hash([20]byte{}))
	ps := ls.(*LayerStore).pieces[0]

	err := ps.MarkNotComplete()
	assert.Nil(err)

	ioutil.WriteFile(ps.ls.pieceStatusPath(), []byte{dirty}, perm)
	err = ps.MarkNotComplete()
	assert.Nil(err)
}

func TestGetIsComplete(t *testing.T) {
	assert := require.New(t)
	info := metainfo.Info{
		Name:   "06",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	_, m := getManager()
	ls, _ := m.OpenTorrent(&info, metainfo.Hash([20]byte{}))
	ps := ls.(*LayerStore).pieces[0]

	assert.False(ps.GetIsComplete())
	ioutil.WriteFile(ps.ls.pieceStatusPath(), []byte{done}, perm)
	assert.True(ps.GetIsComplete())
}
