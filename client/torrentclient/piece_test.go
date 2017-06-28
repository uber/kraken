package torrentclient

import (
	"log"
	"testing"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/store/base"
	"code.uber.internal/infra/kraken/configuration"

	"code.uber.internal/infra/kraken-torrent/metainfo"
	"code.uber.internal/infra/kraken-torrent/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getManager() (*configuration.Config, *Manager) {
	c, s := getFileStore()
	m := NewManager(c, s)
	return c, m
}

func getTorrent(info *metainfo.Info) (storage.TorrentImpl, *configuration.Config) {
	c, m := getManager()
	hash := metainfo.Hash([20]byte{})
	torrent, err := m.OpenTorrent(info, hash)
	if err != nil {
		log.Fatal(err)
	}
	return torrent, c
}

func TestGetOffset(t *testing.T) {
	info := &metainfo.Info{
		Name:   "01",
		Length: int64(1 + 512000),
		Pieces: make([]byte, 40),
	}
	tor, c := getTorrent(info)
	defer removeTestTorrentDirs(c)
	p0 := tor.Piece(info.Piece(0))
	assert.Equal(t, int64(3), p0.(*Piece).getOffset(3))

	p1 := tor.Piece(info.Piece(1))
	assert.Equal(t, int64(512000), p1.(*Piece).getOffset(0))
}

func TestWriteAt(t *testing.T) {
	assert := require.New(t)
	info := &metainfo.Info{
		Name:   "02",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	tor, c := getTorrent(info)
	defer removeTestTorrentDirs(c)
	p0 := tor.Piece(info.Piece(0)).(*Piece)

	// write succeeded
	assert.Equal(0, p0.index)
	n, err := p0.WriteAt([]byte{uint8(7)}, 0)
	assert.Nil(err)
	assert.Equal(1, n)
	status, err := p0.store.GetFilePieceStatus("02", 0, 1)
	assert.Nil(err)
	assert.Equal(store.PieceClean, status[0])
	reader, err := p0.store.GetDownloadOrCacheFileReader("02")
	assert.Nil(err)
	data := make([]byte, 1)
	n, err = reader.Read(data)
	assert.Nil(err)
	assert.Equal(1, n)
	assert.Equal(uint8(7), data[0])
	reader.Close()

	// write failed
	p0.store.WriteDownloadFilePieceStatusAt("02", []byte{store.PieceDirty}, p0.index)
	defer p0.store.WriteDownloadFilePieceStatusAt("02", []byte{store.PieceClean}, p0.index)
	n, err = p0.WriteAt([]byte{uint8(8)}, 0)
	assert.NotNil(err)
	assert.Equal("Another thread is writing to the same piece 02: 0", err.Error())
	assert.Equal(0, n)
	reader, err = p0.store.GetDownloadOrCacheFileReader("02")
	assert.Nil(err)
	n, err = reader.Read(data)
	assert.Nil(err)
	assert.Equal(1, n)
	// no change
	assert.Equal(uint8(7), data[0])
	reader.Close()
	status, err = p0.store.GetFilePieceStatus("02", 0, 1)
	assert.Nil(err)
	assert.Equal(store.PieceDirty, status[0])
}

func TestReadAt(t *testing.T) {
	assert := require.New(t)
	info := &metainfo.Info{
		Name:   "03",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	tor, c := getTorrent(info)
	defer removeTestTorrentDirs(c)
	p0 := tor.Piece(info.Piece(0)).(*Piece)

	// download
	p0.WriteAt([]byte{uint8(7)}, 0)
	bu := make([]byte, 1)
	p0.ReadAt(bu, 0)
	assert.Equal(uint8(7), bu[0])

	// cache
	p0.WriteAt([]byte{uint8(8)}, 0)
	assert.Nil(p0.store.MoveDownloadFileToCache("03"))
	p0.ReadAt(bu, 0)
	assert.Equal(uint8(8), bu[0])

}

func TestMarkComplete(t *testing.T) {
	assert := require.New(t)
	info := &metainfo.Info{
		Name:   "04",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	tor, c := getTorrent(info)
	defer removeTestTorrentDirs(c)
	p0 := tor.Piece(info.Piece(0)).(*Piece)

	assert.Nil(p0.MarkNotComplete())
	status, err := p0.store.GetFilePieceStatus("04", 0, 1)
	assert.Nil(err)
	assert.Equal(store.PieceClean, status[0])

	assert.Nil(p0.MarkComplete())
	status, err = p0.store.GetFilePieceStatus("04", 0, 1)
	assert.Nil(err)
	assert.Equal(store.PieceDone, status[0])
}

func TestMarkNotCompleteCache(t *testing.T) {
	assert := require.New(t)
	info := &metainfo.Info{
		Name:   "05",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	tor, c := getTorrent(info)
	defer removeTestTorrentDirs(c)
	p0 := tor.Piece(info.Piece(0)).(*Piece)

	assert.Nil(p0.MarkComplete())
	status, err := p0.store.GetFilePieceStatus("05", 0, 1)
	assert.Nil(err)
	assert.Equal(store.PieceDone, status[0])

	assert.True(base.IsFileStateError(p0.MarkNotComplete()))
	status, err = p0.store.GetFilePieceStatus("05", 0, 1)
	assert.Nil(err)
	assert.Equal(store.PieceDone, status[0])
}

func TestMarkNotComplete(t *testing.T) {
	assert := require.New(t)
	info := &metainfo.Info{
		Name:   "06",
		Length: int64(1),
		Pieces: make([]byte, 40),
	}
	tor, c := getTorrent(info)
	defer removeTestTorrentDirs(c)
	p0 := tor.Piece(info.Piece(0)).(*Piece)

	assert.Nil(p0.MarkComplete())
	status, err := p0.store.GetFilePieceStatus("06", 0, 1)
	assert.Nil(err)
	assert.Equal(store.PieceDone, status[0])

	assert.Nil(p0.MarkNotComplete())
	status, err = p0.store.GetFilePieceStatus("06", 0, 1)
	assert.Nil(err)
	assert.Equal(store.PieceClean, status[0])
}

func TestGetIsComplete(t *testing.T) {
	assert := require.New(t)
	info := &metainfo.Info{
		Name:   "07",
		Length: int64(1),
		Pieces: make([]byte, 20),
	}
	tor, c := getTorrent(info)
	defer removeTestTorrentDirs(c)
	p0 := tor.Piece(info.Piece(0)).(*Piece)

	assert.False(p0.GetIsComplete())
	p0.store.WriteDownloadFilePieceStatusAt("07", []byte{store.PieceDone}, p0.index)
	assert.True(p0.GetIsComplete())

	p0.store.WriteDownloadFilePieceStatusAt("07", []byte{store.PieceClean}, p0.index)
	assert.False(p0.GetIsComplete())
	p0.store.MoveDownloadFileToCache("07")
	assert.True(p0.GetIsComplete())
}
