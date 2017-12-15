package storage

import (
	"bytes"
	"testing"

	"code.uber.internal/infra/kraken/torlib"
	"github.com/stretchr/testify/require"
)

func TestOriginTorrentArchiveStatNotExist(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newOriginTorrentArchive()

	name := torlib.MetaInfoFixture().Name()

	_, err := archive.Stat(name)
	require.Error(err)
}

func TestOriginTorrentArchiveStatLazilyPullsMetadata(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newOriginTorrentArchive()

	tf := torlib.CustomTestTorrentFileFixture(4, 1)
	mi := tf.MetaInfo

	require.NoError(mocks.fs.CreateCacheFile(mi.Name(), bytes.NewBuffer(tf.Content)))

	mocks.metaInfoClient.EXPECT().Download(mi.Name()).Return(mi, nil).Times(1)

	info, err := archive.Stat(mi.Name())
	require.NoError(err)
	require.Equal(Bitfield{true, true, true, true}, info.Bitfield())
	require.Equal(int64(1), info.MaxPieceLength())
}

func TestOriginTorrentArchiveGetTorrentNotExist(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newOriginTorrentArchive()

	name := torlib.MetaInfoFixture().Name()

	_, err := archive.GetTorrent(name)
	require.Error(err)
}

func TestOriginTorrentArchiveGetTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newOriginTorrentArchive()

	tf := torlib.CustomTestTorrentFileFixture(4, 1)
	mi := tf.MetaInfo

	require.NoError(mocks.fs.CreateCacheFile(mi.Name(), bytes.NewBuffer(tf.Content)))

	mocks.metaInfoClient.EXPECT().Download(mi.Name()).Return(mi, nil).Times(1)

	tor, err := archive.GetTorrent(mi.Name())
	require.NoError(err)
	require.True(tor.Complete())
}
