package storage

import (
	"bytes"
	"testing"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func TestOriginTorrentArchiveStatNotExist(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.OriginFileStoreFixture(clock.New())
	defer cleanup()

	archive := NewOriginTorrentArchive(fs)

	name := torlib.MetaInfoFixture().Name()

	_, err := archive.Stat(name)
	require.Error(err)
}

func TestOriginTorrentArchiveGetTorrentNotExist(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.OriginFileStoreFixture(clock.New())
	defer cleanup()

	archive := NewOriginTorrentArchive(fs)

	name := torlib.MetaInfoFixture().Name()

	_, err := archive.GetTorrent(name)
	require.Error(err)
}

func TestOriginTorrentArchiveGetTorrent(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.OriginFileStoreFixture(clock.New())
	defer cleanup()

	archive := NewOriginTorrentArchive(fs)

	tf := torlib.CustomTestTorrentFileFixture(4, 1)
	mi := tf.MetaInfo

	require.NoError(fs.CreateCacheFile(mi.Name(), bytes.NewBuffer(tf.Content)))

	miRaw, err := mi.Serialize()
	require.NoError(err)
	_, err = fs.SetCacheFileMetadata(mi.Name(), store.NewTorrentMeta(), miRaw)
	require.NoError(err)

	tor, err := archive.GetTorrent(mi.Name())
	require.NoError(err)
	require.True(tor.Complete())
}
