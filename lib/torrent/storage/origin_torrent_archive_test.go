package storage

import (
	"bytes"
	"os"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func setupOriginTorrent(t *testing.T, fs store.OriginFileStore, mi *core.MetaInfo, content []byte) {
	require := require.New(t)

	require.NoError(fs.CreateCacheFile(mi.Name(), bytes.NewBuffer(content)))

	miRaw, err := mi.Serialize()
	require.NoError(err)
	_, err = fs.SetCacheFileMetadata(mi.Name(), store.NewTorrentMeta(), miRaw)
	require.NoError(err)
}

func TestOriginTorrentArchiveStatNotExist(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.OriginFileStoreFixture(clock.New())
	defer cleanup()

	archive := NewOriginTorrentArchive(fs)

	name := core.MetaInfoFixture().Name()

	_, err := archive.Stat(name)
	require.True(os.IsNotExist(err))
}

func TestOriginTorrentArchiveGetTorrentNotExist(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.OriginFileStoreFixture(clock.New())
	defer cleanup()

	archive := NewOriginTorrentArchive(fs)

	name := core.MetaInfoFixture().Name()

	_, err := archive.GetTorrent(name)
	require.Error(err)
}

func TestOriginTorrentArchiveGetTorrent(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.OriginFileStoreFixture(clock.New())
	defer cleanup()

	archive := NewOriginTorrentArchive(fs)

	tf := core.CustomTestTorrentFileFixture(4, 1)
	mi := tf.MetaInfo

	setupOriginTorrent(t, fs, mi, tf.Content)

	tor, err := archive.GetTorrent(mi.Name())
	require.NoError(err)
	require.True(tor.Complete())
}

func TestOriginTorrentArchiveDeleteTorrent(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.OriginFileStoreFixture(clock.New())
	defer cleanup()

	archive := NewOriginTorrentArchive(fs)

	tf := core.CustomTestTorrentFileFixture(4, 1)
	mi := tf.MetaInfo

	setupOriginTorrent(t, fs, mi, tf.Content)

	_, err := archive.Stat(mi.Name())
	require.NoError(err)

	require.NoError(archive.DeleteTorrent(mi.Name()))

	_, err = archive.Stat(mi.Name())
	require.True(os.IsNotExist(err))
}
