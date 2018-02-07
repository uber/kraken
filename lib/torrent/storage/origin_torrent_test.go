package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path"
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func TestOriginTorrentCreate(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.OriginFileStoreFixture(clock.New())
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo

	fs.CreateCacheFile(mi.Name(), bytes.NewReader(tf.Content))

	tor, err := NewOriginTorrent(fs, mi)
	require.NoError(err)

	// New torrent
	require.Equal(path.Base(mi.Name()), tor.Name())
	require.Equal(4, tor.NumPieces())
	require.Equal(int64(7), tor.Length())
	require.Equal(int64(2), tor.PieceLength(0))
	require.Equal(int64(1), tor.PieceLength(3))
	require.Equal(mi.InfoHash, tor.InfoHash())
	require.True(tor.Complete())
	require.Equal(int64(7), tor.BytesDownloaded())
	require.Equal(BitSetFixture(true, true, true, true), tor.Bitfield())
	require.Equal(fmt.Sprintf("torrent(hash=%s, downloaded=100%%)", mi.InfoHash.HexString()), tor.String())
	require.True(tor.HasPiece(0))
	require.Equal([]int{}, tor.MissingPieces())
}

func TestOriginTorrentGetPieceReaderConcurrent(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.OriginFileStoreFixture(clock.New())
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo

	fs.CreateCacheFile(mi.Name(), bytes.NewReader(tf.Content))

	tor, err := NewOriginTorrent(fs, mi)
	require.NoError(err)

	wg := sync.WaitGroup{}
	wg.Add(tor.NumPieces())
	for i := 0; i < tor.NumPieces(); i++ {
		go func(i int) {
			defer wg.Done()
			start := i * int(mi.Info.PieceLength)
			end := start + int(tor.PieceLength(i))
			r, err := tor.GetPieceReader(i)
			require.NoError(err)
			defer r.Close()
			result, err := ioutil.ReadAll(r)
			require.NoError(err)
			require.Equal(tf.Content[start:end], result)
		}(i)
	}

	wg.Wait()
}

func TestOriginTorrentWritePieceError(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.OriginFileStoreFixture(clock.New())
	defer cleanup()

	tf := torlib.CustomTestTorrentFileFixture(7, 2)
	mi := tf.MetaInfo

	fs.CreateCacheFile(mi.Name(), bytes.NewReader(tf.Content))

	tor, err := NewOriginTorrent(fs, mi)
	require.NoError(err)

	err = tor.WritePiece([]byte{}, 0)
	require.Equal(ErrReadOnly, err)
}
