package storage

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"path"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/store"
	"code.uber.internal/infra/kraken/torlib"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestLocalTorrentCreate(t *testing.T) {
	require := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo

	tor, err := archive.CreateTorrent(mi)
	require.NoError(err)

	// New torrent
	require.Equal(path.Base(mi.Name()), tor.Name())
	require.Equal(4, tor.NumPieces())
	require.Equal(int64(7), tor.Length())
	require.Equal(int64(2), tor.PieceLength(0))
	require.Equal(int64(1), tor.PieceLength(3))
	require.Equal(mi.InfoHash, tor.InfoHash())
	require.False(tor.Complete())
	require.Equal(int64(0), tor.BytesDownloaded())
	require.Equal(Bitfield{false, false, false, false}, tor.Bitfield())
	require.Equal(fmt.Sprintf("torrent(hash=%s, downloaded=0%%)", mi.InfoHash.HexString()), tor.String())
	require.False(tor.HasPiece(0))
	require.Equal([]int{0, 1, 2, 3}, tor.MissingPieces())
}

func TestLocalTorrentWriteUpdatesBytesDownloadedAndBitfield(t *testing.T) {
	require := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(2, 1)
	mi := tf.MetaInfo
	data := tf.Content

	tor, err := archive.CreateTorrent(mi)
	require.NoError(err)

	require.NoError(tor.WritePiece(data[:1], 0))
	require.False(tor.Complete())
	require.Equal(int64(1), tor.BytesDownloaded())
	require.Equal(Bitfield{true, false}, tor.Bitfield())
}

func TestLocalTorrentWriteComplete(t *testing.T) {
	require := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(1, 1)
	mi := tf.MetaInfo
	data := tf.Content

	tor, err := archive.CreateTorrent(mi)
	require.NoError(err)

	require.NoError(tor.WritePiece(data, 0))

	readPiece, err := tor.ReadPiece(0)
	require.NoError(err)
	require.Equal(readPiece, data)

	require.True(tor.Complete())
	require.Equal(int64(1), tor.BytesDownloaded())

	// Duplicate write should detect piece is complete.
	require.Error(ErrPieceComplete, tor.WritePiece(data[:1], 0))
}

func TestLocalTorrentWriteMultiplePieceConcurrent(t *testing.T) {
	require := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo
	data := tf.Content

	tor, err := archive.CreateTorrent(mi)
	require.NoError(err)

	wg := sync.WaitGroup{}
	wg.Add(tor.NumPieces())
	for i := 0; i < tor.NumPieces(); i++ {
		go func(i int) {
			defer wg.Done()
			start := i * int(mi.Info.PieceLength)
			end := start + int(tor.PieceLength(i))
			require.NoError(tor.WritePiece(data[start:end], i))
		}(i)
	}

	wg.Wait()

	// Complete
	require.True(tor.Complete())
	require.Equal(int64(7), tor.BytesDownloaded())
	require.Nil(tor.MissingPieces())
	require.Equal(fmt.Sprintf("torrent(hash=%s, downloaded=100%%)", mi.InfoHash.HexString()), tor.String())

	// Check content
	reader, err := archive.(*LocalTorrentArchive).store.GetCacheFileReader(mi.Name())
	require.NoError(err)
	torrentBytes, err := ioutil.ReadAll(reader)
	require.NoError(err)
	require.Equal(data, torrentBytes)
}

func TestLocalTorrentWriteSamePieceConcurrent(t *testing.T) {
	require := require.New(t)
	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(16, 1)
	mi := tf.MetaInfo
	data := tf.Content

	tor, err := archive.CreateTorrent(mi)
	require.NoError(err)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			pi := int(math.Mod(float64(i), float64(len(data))))

			err := tor.WritePiece([]byte{data[pi]}, pi)
			if err != nil && err != ErrWritePieceConflict && err != ErrPieceComplete {
				require.Equal(ErrWritePieceConflict, err)
			}

			time.Sleep(5 * time.Millisecond)

			result, err := tor.ReadPiece(pi)
			require.NoError(err)
			require.Equal(1, len(result))
			require.Equal(data[pi], result[0])
		}(i)
	}
	wg.Wait()

	reader, err := archive.(*LocalTorrentArchive).store.GetCacheFileReader(mi.Name())
	require.NoError(err)
	torrentBytes, err := ioutil.ReadAll(reader)
	require.NoError(err)
	require.Equal(data, torrentBytes)
}

// coordinatedWriter allows blocking WriteAt calls to simulate race conditions.
type coordinatedWriter struct {
	store.FileReadWriter
	startWriting chan bool
	stopWriting  chan bool
}

func newCoordinatedWriter(f store.FileReadWriter) *coordinatedWriter {
	return &coordinatedWriter{f, make(chan bool), make(chan bool)}
}

func (w *coordinatedWriter) WriteAt([]byte, int64) (int, error) {
	w.startWriting <- true
	<-w.stopWriting
	return 0, nil
}

func TestLocalTorrentWritePieceConflictsDoNotBlock(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fs := mockstore.NewMockFileStore(ctrl)

	tf := torlib.CustomTestTorrentFileFixture(1, 1)

	f, cleanup := store.NewMockFileReadWriter([]byte{})
	defer cleanup()

	fs.EXPECT().GetDownloadOrCacheFileReader(tf.MetaInfo.Info.Name).Return(f, nil)

	tor := NewLocalTorrent(fs, tf.MetaInfo)

	w := newCoordinatedWriter(f)
	fs.EXPECT().GetDownloadFileReadWriter(tf.MetaInfo.Info.Name).Return(w, nil).AnyTimes()
	fs.EXPECT().MoveDownloadFileToCache(tf.MetaInfo.Info.Name).Return(nil)

	done := make(chan struct{})
	go func() {
		require.NoError(tor.WritePiece(tf.Content, 0))
		close(done)
	}()

	// Writing while another goroutine is mid-write should not block.
	<-w.startWriting
	require.Equal(ErrWritePieceConflict, tor.WritePiece(tf.Content, 0))
	w.stopWriting <- true

	<-done

	// Duplicate write should detect piece is complete.
	require.Error(ErrPieceComplete, tor.WritePiece(tf.Content, 0))
}

func TestLocalTorrentWritePieceFailuresRemoveDirtyStatus(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fs := mockstore.NewMockFileStore(ctrl)

	tf := torlib.CustomTestTorrentFileFixture(1, 1)

	// Ensure restorePieces does not restore the piece.
	r := mockstore.NewMockFileReadWriter(ctrl)
	fs.EXPECT().GetDownloadOrCacheFileReader(tf.MetaInfo.Info.Name).Return(r, nil)
	r.EXPECT().ReadAt(gomock.Any(), int64(0)).Return(0, errors.New("read error"))
	r.EXPECT().Close().Return(nil)

	w := mockstore.NewMockFileReadWriter(ctrl)
	gomock.InOrder(
		// First write fails.
		fs.EXPECT().GetDownloadFileReadWriter(tf.MetaInfo.Info.Name).Return(w, nil),
		w.EXPECT().WriteAt(tf.Content, int64(0)).Return(0, errors.New("first write error")),
		w.EXPECT().Close().Return(nil),

		// Second write succeeds.
		fs.EXPECT().GetDownloadFileReadWriter(tf.MetaInfo.Info.Name).Return(w, nil),
		w.EXPECT().WriteAt(tf.Content, int64(0)).Return(0, nil),
		w.EXPECT().Close().Return(nil),
		fs.EXPECT().MoveDownloadFileToCache(tf.MetaInfo.Info.Name).Return(nil),
	)

	tor := NewLocalTorrent(fs, tf.MetaInfo)

	// After the first write fails, the dirty bit should be flipped to empty,
	// allowing future writes to succeed.
	require.Error(tor.WritePiece(tf.Content, 0))
	require.NoError(tor.WritePiece(tf.Content, 0))
}
