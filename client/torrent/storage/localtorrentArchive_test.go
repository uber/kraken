package storage

import (
	"os"
	"testing"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/torlib"
	"github.com/stretchr/testify/require"
)

func TestLocalTorrentArchiveCreateTorrent(t *testing.T) {
	assert := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo

	_, err := archive.CreateTorrent(mi.InfoHash, mi)
	assert.Nil(err)

	// Check metainfo
	miRaw, err := archive.(*LocalTorrentArchive).store.GetDownloadOrCacheFileMeta(mi.Name())
	assert.Nil(err)
	miExpected, err := mi.Serialize()
	assert.Nil(err)
	assert.Equal(miExpected, string(miRaw))

	// Check piece statuses
	statuses, err := archive.(*LocalTorrentArchive).store.GetFilePieceStatus(mi.Name(), 0, mi.Info.NumPieces())
	assert.Nil(err)
	for i := 0; i < mi.Info.NumPieces(); i++ {
		assert.Equal(store.PieceClean, statuses[i])
	}

	// Verify exist
	_, err = archive.GetTorrent(mi.Name(), mi.InfoHash)
	assert.Nil(err)

	// Create again is ok
	_, err = archive.CreateTorrent(mi.InfoHash, mi)
	assert.Nil(err)
}

func TestLocalTorrentArchiveGetTorrentNotFound(t *testing.T) {
	assert := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo

	tor, err := archive.GetTorrent(mi.Name(), mi.InfoHash)
	assert.NotNil(err)
	assert.True(os.IsNotExist(err))
	assert.Nil(tor)
	assert.True(os.IsNotExist(archive.DeleteTorrent(mi.Name(), mi.InfoHash)))
}

func TestLocalTorrentArchiveGetTorrentInfoHashMissMatch(t *testing.T) {
	assert := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo

	_, err := archive.CreateTorrent(mi.InfoHash, mi)
	assert.Nil(err)

	badInfoHash := torlib.NewInfoHashFromBytes([]byte{})
	tor, err := archive.GetTorrent(mi.Name(), badInfoHash)
	assert.NotNil(err)
	assert.True(IsInfoHashMissMatchError(err))
	assert.Equal(err.(InfoHashMissMatchError).expected, badInfoHash)
	assert.Equal(err.(InfoHashMissMatchError).actual, mi.InfoHash)
	assert.Nil(tor)
}
func TestLocalTorrentArchiveDeleteTorrent(t *testing.T) {
	assert := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo

	// Torrent exists
	_, err := archive.CreateTorrent(mi.InfoHash, mi)
	assert.Nil(err)

	// Verify exists
	_, err = archive.GetTorrent(mi.Name(), mi.InfoHash)
	assert.Nil(err)

	// Delete torrent
	assert.Nil(archive.DeleteTorrent(mi.Name(), mi.InfoHash))

	// Confirm deleted
	_, err = archive.GetTorrent(mi.Name(), mi.InfoHash)
	assert.NotNil(err)
	assert.True(os.IsNotExist(err))
}
