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
package originstorage

import (
	"bytes"
	"io/ioutil"
	"sync"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"
	"github.com/uber/kraken/utils/bitsetutil"

	"github.com/stretchr/testify/require"
)

func TestTorrentCreate(t *testing.T) {
	require := require.New(t)

	cas, cleanup := store.CAStoreFixture()
	defer cleanup()

	blob := core.SizedBlobFixture(7, 2)
	mi := blob.MetaInfo

	cas.CreateCacheFile(mi.Digest().Hex(), bytes.NewReader(blob.Content))

	tor, err := NewTorrent(cas, mi)
	require.NoError(err)

	// New torrent
	require.Equal(mi.Digest(), tor.Digest())
	require.Equal(4, tor.NumPieces())
	require.Equal(int64(7), tor.Length())
	require.Equal(int64(2), tor.PieceLength(0))
	require.Equal(int64(1), tor.PieceLength(3))
	require.Equal(mi.InfoHash(), tor.InfoHash())
	require.True(tor.Complete())
	require.Equal(int64(7), tor.BytesDownloaded())
	require.Equal(bitsetutil.FromBools(true, true, true, true), tor.Bitfield())
	require.True(tor.HasPiece(0))
	require.Equal([]int{}, tor.MissingPieces())
}

func TestTorrentGetPieceReaderConcurrent(t *testing.T) {
	require := require.New(t)

	cas, cleanup := store.CAStoreFixture()
	defer cleanup()

	blob := core.SizedBlobFixture(7, 2)
	mi := blob.MetaInfo

	cas.CreateCacheFile(mi.Digest().Hex(), bytes.NewReader(blob.Content))

	tor, err := NewTorrent(cas, mi)
	require.NoError(err)

	wg := sync.WaitGroup{}
	wg.Add(tor.NumPieces())
	for i := 0; i < tor.NumPieces(); i++ {
		go func(i int) {
			defer wg.Done()
			start := i * int(mi.PieceLength())
			end := start + int(tor.PieceLength(i))
			r, err := tor.GetPieceReader(i)
			require.NoError(err)
			defer r.Close()
			result, err := ioutil.ReadAll(r)
			require.NoError(err)
			require.Equal(blob.Content[start:end], result)
		}(i)
	}

	wg.Wait()
}

func TestTorrentWritePieceError(t *testing.T) {
	require := require.New(t)

	cas, cleanup := store.CAStoreFixture()
	defer cleanup()

	blob := core.SizedBlobFixture(7, 2)
	mi := blob.MetaInfo

	cas.CreateCacheFile(mi.Digest().Hex(), bytes.NewReader(blob.Content))

	tor, err := NewTorrent(cas, mi)
	require.NoError(err)

	err = tor.WritePiece(piecereader.NewBuffer([]byte{}), 0)
	require.Equal(ErrReadOnly, err)
}
