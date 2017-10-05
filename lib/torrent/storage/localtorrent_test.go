package storage

import (
	"fmt"
	"io/ioutil"
	"math"
	"path"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"

	"github.com/stretchr/testify/require"
)

func TestLocalTorrentCreate(t *testing.T) {
	assert := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo

	tor, err := archive.CreateTorrent(mi.InfoHash, mi)
	assert.Nil(err)

	// New torrent
	assert.Equal(path.Base(mi.Name()), tor.Name())
	assert.Equal(4, tor.NumPieces())
	assert.Equal(int64(7), tor.Length())
	assert.Equal(int64(2), tor.PieceLength(0))
	assert.Equal(int64(1), tor.PieceLength(3))
	assert.Equal(mi.InfoHash, tor.InfoHash())
	assert.False(tor.Complete())
	assert.Equal(int64(0), tor.BytesDownloaded())
	assert.Equal(Bitfield{false, false, false, false}, tor.Bitfield())
	assert.Equal(fmt.Sprintf("torrent(hash=%s, bitfield=0000)", mi.InfoHash.HexString()), tor.String())
	assert.False(tor.HasPiece(0))
	assert.Equal([]int{0, 1, 2, 3}, tor.MissingPieces())
}

func TestLocalTorrentWrite(t *testing.T) {
	assert := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(2, 1)
	mi := tf.MetaInfo
	data := tf.Content

	tor, err := archive.CreateTorrent(mi.InfoHash, mi)
	assert.Nil(err)

	n, err := tor.WritePiece(data[:1], 0)
	assert.Nil(err)
	assert.Equal(1, n)
	assert.False(tor.Complete())
	assert.Equal(int64(1), tor.BytesDownloaded())
	assert.Equal(Bitfield{true, false}, tor.Bitfield())
}

func TestLocalTorrentWriteComplete(t *testing.T) {
	assert := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(1, 1)
	mi := tf.MetaInfo
	data := tf.Content

	tor, err := archive.CreateTorrent(mi.InfoHash, mi)
	assert.Nil(err)

	n, err := tor.WritePiece(data, 0)
	assert.Nil(err)
	assert.Equal(1, n)

	readPiece, err := tor.ReadPiece(0)
	assert.Nil(err)
	assert.Equal(readPiece, data)

	assert.True(tor.Complete())
	assert.Equal(int64(1), tor.BytesDownloaded())

	_, err = tor.WritePiece(data[:1], 0)
	assert.NotNil(err)
}

func TestLocalTorrentWriteMultiplePieceConcurrent(t *testing.T) {
	assert := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo
	data := tf.Content

	tor, err := archive.CreateTorrent(mi.InfoHash, mi)
	assert.Nil(err)

	wg := sync.WaitGroup{}
	wg.Add(tor.NumPieces())
	for i := 0; i < tor.NumPieces(); i++ {
		go func(i int) {
			defer wg.Done()
			start := i * int(mi.Info.PieceLength)
			end := start + int(tor.PieceLength(i))
			_, err := tor.WritePiece(data[start:end], i)
			assert.Nil(err)
		}(i)
	}

	wg.Wait()

	// Complete
	assert.True(tor.Complete())
	assert.Equal(int64(7), tor.BytesDownloaded())
	assert.Nil(tor.MissingPieces())
	assert.Equal(fmt.Sprintf("torrent(hash=%s, bitfield=1111)", mi.InfoHash.HexString()), tor.String())

	// Check content
	reader, err := archive.(*LocalTorrentArchive).store.GetCacheFileReader(mi.Name())
	assert.Nil(err)
	torrentBytes, err := ioutil.ReadAll(reader)
	assert.Nil(err)
	assert.Equal(data, torrentBytes)
}

func TestLocalTorrentWriteSamePieceConcurrent(t *testing.T) {
	assert := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(128, 1)
	mi := tf.MetaInfo
	data := tf.Content

	tor, err := archive.CreateTorrent(mi.InfoHash, mi)
	assert.Nil(err)

	wg := &sync.WaitGroup{}
	for i := 0; i < 250; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pieceIndex := int(math.Mod(float64(i), float64(len(data))))
			tor.WritePiece([]byte{data[pieceIndex]}, pieceIndex)
			time.Sleep(5 * time.Millisecond)
			readData, err := tor.ReadPiece(pieceIndex)
			assert.NoError(err)
			assert.Equal(1, len(readData))
			assert.Equal(data[pieceIndex], readData[0])
		}(i)
	}

	wg.Wait()
	reader, err := archive.(*LocalTorrentArchive).store.GetCacheFileReader(mi.Name())
	assert.Nil(err)
	torrentBytes, err := ioutil.ReadAll(reader)
	assert.Nil(err)
	assert.Equal(data, torrentBytes)
}

func TestPrepareAndFinishPieceWrite(t *testing.T) {
	assert := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 7)
	mi := tf.MetaInfo

	tor, err := archive.CreateTorrent(mi.InfoHash, mi)
	assert.Nil(err)

	localTor := tor.(*LocalTorrent)
	assert.Nil(localTor.preparePieceWrite(0))
	// Check piece status, should be dirty
	statuses, err := localTor.pieceStatuses()
	assert.Nil(err)
	assert.Equal(store.PieceDirty, statuses[0])

	// Calling preparePieceWrite again before finishPieceWrite should fail
	err = localTor.preparePieceWrite(0)
	assert.NotNil(err)
	assert.True(IsConflictedPieceWriteError(err))
	assert.Equal(err.(ConflictedPieceWriteError).torrent, mi.Name())
	assert.Equal(err.(ConflictedPieceWriteError).piece, 0)
	// Succeeded after finishPieceWrite
	assert.Nil(localTor.finishPieceWrite(0, false))
	// Check piece status, should be clean
	statuses, err = localTor.pieceStatuses()
	assert.Nil(err)
	assert.Equal(store.PieceClean, statuses[0])

	assert.Nil(localTor.preparePieceWrite(0))
	// Finish and mark piece complete
	assert.Nil(localTor.finishPieceWrite(0, true))
	// Check piece status, should be done
	statuses, err = localTor.pieceStatuses()
	assert.Nil(err)
	assert.Equal(store.PieceDone, statuses[0])

	// Cannot write after done
	assert.NotNil(localTor.preparePieceWrite(0))
}
