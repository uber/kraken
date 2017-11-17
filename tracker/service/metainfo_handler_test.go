package service

import (
	"testing"

	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/tracker/storage"
	"github.com/stretchr/testify/require"
)

func TestUploadAndDownloadMetaInfo(t *testing.T) {
	require := require.New(t)

	mocks, finish := newTestMocks(t)
	defer finish()

	addr, stop := mocks.startServer()
	defer stop()

	client := metainfoclient.Default(serverset.NewSingle(addr))

	mi := torlib.MetaInfoFixture()
	serialized, err := mi.Serialize()
	require.NoError(err)

	mocks.datastore.EXPECT().CreateTorrent(mi).Return(nil)

	require.NoError(client.Upload(mi))

	mocks.datastore.EXPECT().GetTorrent(mi.Name()).Return(string(serialized), nil)

	result, err := client.Download(mi.Name())
	require.NoError(err)
	require.Equal(mi, result)
}

func TestUploadMetaInfoConflict(t *testing.T) {
	require := require.New(t)

	mocks, finish := newTestMocks(t)
	defer finish()

	addr, stop := mocks.startServer()
	defer stop()

	client := metainfoclient.Default(serverset.NewSingle(addr))

	mi := torlib.MetaInfoFixture()

	mocks.datastore.EXPECT().CreateTorrent(mi).Return(nil)

	require.NoError(client.Upload(mi))

	mocks.datastore.EXPECT().CreateTorrent(mi).Return(storage.ErrExists)

	require.Equal(metainfoclient.ErrExists, client.Upload(mi))
}
