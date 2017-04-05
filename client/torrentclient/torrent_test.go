package torrentclient

import (
	"testing"

	"os"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"
)

func TestNewTorrent(t *testing.T) {
	assert := require.New(t)
	c, s := getFileStore()
	defer os.RemoveAll(c.DownloadDir)

	tor := NewTorrent(c, s, "t0", int64(1), 1)
	assert.Equal("t0", tor.name)
	assert.Equal(int64(1), tor.len)
	assert.Equal(1, tor.numPieces)
	assert.Nil(tor.Close())
}

func TestPiece(t *testing.T) {
	assert := require.New(t)
	c, s := getFileStore()
	defer os.RemoveAll(c.DownloadDir)

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
