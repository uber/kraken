// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package agentstorage

import (
	"os"
	"sync"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"
	"github.com/uber/kraken/mocks/tracker/metainfoclient"
	"github.com/uber/kraken/tracker/metainfoclient"
	"github.com/uber/kraken/utils/bitsetutil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const pieceLength = 4

type archiveMocks struct {
	cads           *store.CADownloadStore
	metaInfoClient *mockmetainfoclient.MockClient
}

func newArchiveMocks(t *testing.T) (*archiveMocks, func()) {
	var cleanup testutil.Cleanup

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	cads, c := store.CADownloadStoreFixture()
	cleanup.Add(c)

	metaInfoClient := mockmetainfoclient.NewMockClient(ctrl)

	return &archiveMocks{cads, metaInfoClient}, cleanup.Run
}

func (m *archiveMocks) new() *TorrentArchive {
	return NewTorrentArchive(tally.NoopScope, m.cads, m.metaInfoClient)
}

func TestTorrentArchiveStatBitfield(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new()

	namespace := core.TagFixture()
	blob := core.SizedBlobFixture(4, 1)
	mi := blob.MetaInfo

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Digest()).Return(mi, nil).Times(1)

	tor, err := archive.CreateTorrent(namespace, mi.Digest())
	require.NoError(err)

	require.NoError(tor.WritePiece(piecereader.NewBuffer(blob.Content[2:3]), 2))

	info, err := archive.Stat(namespace, mi.Digest())
	require.NoError(err)
	require.Equal(bitsetutil.FromBools(false, false, true, false), info.Bitfield())
	require.Equal(int64(1), info.MaxPieceLength())
}

func TestTorrentArchiveStatNotExist(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new()

	namespace := core.TagFixture()
	d := core.DigestFixture()

	_, err := archive.Stat(namespace, d)
	require.True(os.IsNotExist(err))
}

func TestTorrentArchiveCreateTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new()

	mi := core.MetaInfoFixture()
	namespace := core.TagFixture()

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Digest()).Return(mi, nil)

	tor, err := archive.CreateTorrent(namespace, mi.Digest())
	require.NoError(err)
	require.NotNil(tor)

	// Check metainfo.
	var tm metadata.TorrentMeta
	require.NoError(mocks.cads.Any().GetMetadata(mi.Digest().Hex(), &tm))
	require.Equal(mi, tm.MetaInfo)

	// Create again reads from disk.
	tor, err = archive.CreateTorrent(namespace, mi.Digest())
	require.NoError(err)
	require.NotNil(tor)
}

func TestTorrentArchiveCreateTorrentNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new()

	mi := core.MetaInfoFixture()
	namespace := core.TagFixture()

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Digest()).Return(nil, metainfoclient.ErrNotFound)

	_, err := archive.CreateTorrent(namespace, mi.Digest())
	require.Equal(storage.ErrNotFound, err)
}

func TestTorrentArchiveDeleteTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new()

	mi := core.MetaInfoFixture()
	namespace := core.TagFixture()

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Digest()).Return(mi, nil)

	tor, err := archive.CreateTorrent(namespace, mi.Digest())
	require.NoError(err)
	require.NotNil(tor)

	require.NoError(archive.DeleteTorrent(mi.Digest()))

	_, err = archive.Stat(namespace, mi.Digest())
	require.True(os.IsNotExist(err))
}

func TestTorrentArchiveConcurrentGet(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new()

	mi := core.MetaInfoFixture()
	namespace := core.TagFixture()

	// Allow any times for concurrency below.
	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Digest()).Return(mi, nil).AnyTimes()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tor, err := archive.CreateTorrent(namespace, mi.Digest())
			require.NoError(err)
			require.NotNil(tor)
		}()
	}
	wg.Wait()
}

func TestTorrentArchiveGetTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new()

	mi := core.MetaInfoFixture()
	namespace := core.TagFixture()

	// Since metainfo is not yet on disk, get should fail.
	_, err := archive.GetTorrent(namespace, mi.Digest())
	require.Error(err)

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Digest()).Return(mi, nil)

	_, err = archive.CreateTorrent(namespace, mi.Digest())
	require.NoError(err)

	// After creating the torrent, get should succeed.
	tor, err := archive.GetTorrent(namespace, mi.Digest())
	require.NoError(err)
	require.NotNil(tor)
}
