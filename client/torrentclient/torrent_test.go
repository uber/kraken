package torrentclient

import (
	"testing"

	"code.uber.internal/infra/kraken/client/store"

	"os"

	"regexp"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"
)

func TestNewTorrent(t *testing.T) {
	assert := require.New(t)
	c, s := getFileStore()
	defer os.RemoveAll(c.DownloadDir)
	defer os.RemoveAll(c.CacheDir)

	tor := NewTorrent(c, s, "t0", int64(1), 1)
	assert.Equal("t0", tor.name)
	assert.Equal(int64(1), tor.len)
	assert.Equal(1, tor.numPieces)
	assert.Nil(tor.Close())
}

func TestPiece(t *testing.T) {
	assert := require.New(t)
	c, s := getFileStore()
	defer removeTestTorrentDirs(c)

	tor := NewTorrent(c, s, "t1", int64(1), 1)
	info := metainfo.Info{
		Name:   "t1",
		Length: int64(1),
		Pieces: make([]byte, 40),
	}
	p0 := tor.Piece(info.Piece(0))
	assert.NotNil(p0)
	p1 := tor.Piece(info.Piece(1))
	assert.Nil(p1)
}

func TestOpenCreated(t *testing.T) {
	assert := require.New(t)
	c, s := getFileStore()
	defer removeTestTorrentDirs(c)

	tor := NewTorrent(c, s, "t2", int64(1), 1)
	new, err := s.CreateDownloadFile(tor.name, tor.len)
	assert.Nil(err)
	assert.True(new)
	assert.Nil(tor.Open())
	// will not call
	_, err = s.GetFilePieceStatus("t2", 0, 1)
	match, _ := regexp.MatchString(".*no such file or directory.*", err.Error())
	assert.True(match)
}

func TestOpenCached(t *testing.T) {
	assert := require.New(t)
	c, s := getFileStore()
	defer removeTestTorrentDirs(c)

	tor := NewTorrent(c, s, "t3", int64(1), 1)
	new, err := s.CreateUploadFile(tor.name, tor.len)
	assert.Nil(err)
	assert.True(new)
	assert.True(store.IsFileStateError(tor.Open()))
}
