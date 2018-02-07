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

func prepareFileStore(fs store.FileStore, mi *torlib.MetaInfo) {
	fs.CreateDownloadFile(mi.Name(), mi.Info.Length)
	b, err := mi.Serialize()
	if err != nil {
		panic(err)
	}
	fs.States().Download().SetMetadata(mi.Name(), store.NewTorrentMeta(), b)
}

func TestAgentTorrentCreate(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo

	prepareFileStore(fs, mi)

	tor, err := NewAgentTorrent(fs, mi)
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
	require.Equal(BitSetFixture(false, false, false, false), tor.Bitfield())
	require.Equal(fmt.Sprintf("torrent(hash=%s, downloaded=0%%)", mi.InfoHash.HexString()), tor.String())
	require.False(tor.HasPiece(0))
	require.Equal([]int{0, 1, 2, 3}, tor.MissingPieces())
}

func TestAgentTorrentWriteUpdatesBytesDownloadedAndBitfield(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(2, 1)
	mi := tf.MetaInfo
	data := tf.Content

	prepareFileStore(fs, mi)

	tor, err := NewAgentTorrent(fs, mi)
	require.NoError(err)

	require.NoError(tor.WritePiece(data[:1], 0))
	require.False(tor.Complete())
	require.Equal(int64(1), tor.BytesDownloaded())
	require.Equal(BitSetFixture(true, false), tor.Bitfield())
}

func TestAgentTorrentWriteComplete(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(1, 1)
	mi := tf.MetaInfo
	data := tf.Content

	prepareFileStore(fs, mi)

	tor, err := NewAgentTorrent(fs, mi)
	require.NoError(err)

	require.NoError(tor.WritePiece(data, 0))

	r, err := tor.GetPieceReader(0)
	require.NoError(err)
	defer r.Close()
	result, err := ioutil.ReadAll(r)
	require.NoError(err)
	require.Equal(data, result)

	require.True(tor.Complete())
	require.Equal(int64(1), tor.BytesDownloaded())

	// Duplicate write should detect piece is complete.
	require.Equal(ErrPieceComplete, tor.WritePiece(data[:1], 0))
}

func TestAgentTorrentWriteMultiplePieceConcurrent(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo
	data := tf.Content

	prepareFileStore(fs, mi)

	tor, err := NewAgentTorrent(fs, mi)
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
	reader, err := fs.GetCacheFileReader(mi.Name())
	require.NoError(err)
	torrentBytes, err := ioutil.ReadAll(reader)
	require.NoError(err)
	require.Equal(data, torrentBytes)
}

func TestAgentTorrentWriteSamePieceConcurrent(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(16, 1)
	mi := tf.MetaInfo
	data := tf.Content

	prepareFileStore(fs, mi)

	tor, err := NewAgentTorrent(fs, mi)
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

			r, err := tor.GetPieceReader(pi)
			require.NoError(err)
			defer r.Close()
			result, err := ioutil.ReadAll(r)
			require.NoError(err)
			require.Equal(1, len(result))
			require.Equal(1, len(result))
			require.Equal(data[pi], result[0])
		}(i)
	}
	wg.Wait()

	reader, err := fs.GetCacheFileReader(mi.Name())
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

func TestAgentTorrentWritePieceConflictsDoNotBlock(t *testing.T) {
	require := require.New(t)

	tf := torlib.CustomTestTorrentFileFixture(1, 1)

	f, cleanup := store.NewMockFileReadWriter([]byte{})
	defer cleanup()

	w := newCoordinatedWriter(f)

	baseFS, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	fs := store.MockGetDownloadFileReadWriter(baseFS, w)

	prepareFileStore(fs, tf.MetaInfo)

	tor, err := NewAgentTorrent(fs, tf.MetaInfo)
	require.NoError(err)

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
	require.Equal(ErrPieceComplete, tor.WritePiece(tf.Content, 0))
}

func TestAgentTorrentWritePieceFailuresRemoveDirtyStatus(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := mockstore.NewMockFileReadWriter(ctrl)

	baseFS, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	fs := store.MockGetDownloadFileReadWriter(baseFS, w)

	tf := torlib.CustomTestTorrentFileFixture(1, 1)

	prepareFileStore(fs, tf.MetaInfo)

	gomock.InOrder(
		// First write fails.
		w.EXPECT().WriteAt(tf.Content, int64(0)).Return(0, errors.New("first write error")),
		w.EXPECT().Close().Return(nil),

		// Second write succeeds.
		w.EXPECT().WriteAt(tf.Content, int64(0)).Return(0, nil),
		w.EXPECT().Close().Return(nil),
	)

	tor, err := NewAgentTorrent(fs, tf.MetaInfo)
	require.NoError(err)

	// After the first write fails, the dirty bit should be flipped to empty,
	// allowing future writes to succeed.
	require.Error(tor.WritePiece(tf.Content, 0))
	require.NoError(tor.WritePiece(tf.Content, 0))
}

func TestAgentTorrentRestoreCompletedTorrent(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(8, 1)

	prepareFileStore(fs, tf.MetaInfo)

	tor, err := NewAgentTorrent(fs, tf.MetaInfo)
	require.NoError(err)

	for i, b := range tf.Content {
		require.NoError(tor.WritePiece([]byte{b}, i))
	}

	require.True(tor.Complete())

	tor, err = NewAgentTorrent(fs, tf.MetaInfo)
	require.NoError(err)

	require.True(tor.Complete())
}

func TestAgentTorrentRestoreInProgressTorrent(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(8, 1)

	prepareFileStore(fs, tf.MetaInfo)

	tor, err := NewAgentTorrent(fs, tf.MetaInfo)
	require.NoError(err)

	pi := 4

	require.NoError(tor.WritePiece([]byte{tf.Content[pi]}, pi))
	require.Equal(int64(1), tor.BytesDownloaded())

	tor, err = NewAgentTorrent(fs, tf.MetaInfo)
	require.NoError(err)

	require.Equal(int64(1), tor.BytesDownloaded())
	require.Equal(ErrPieceComplete, tor.WritePiece([]byte{tf.Content[pi]}, pi))
}
