// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package originstorage

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
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

	require.NoError(cas.CreateCacheFile(mi.Digest().Hex(), bytes.NewReader(blob.Content)))

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

	require.NoError(cas.CreateCacheFile(mi.Digest().Hex(), bytes.NewReader(blob.Content)))

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
			defer func() {
				require.NoError(r.Close())
			}()
			result, err := io.ReadAll(r)
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

	require.NoError(cas.CreateCacheFile(mi.Digest().Hex(), bytes.NewReader(blob.Content)))

	tor, err := NewTorrent(cas, mi)
	require.NoError(err)

	err = tor.WritePiece(piecereader.NewBuffer([]byte{}), 0)
	require.Equal(ErrReadOnly, err)
}

type fakeRangeDownloader struct {
	mu       sync.Mutex
	content  []byte
	calls    int
	failNext bool
	delay    time.Duration
}

func (d *fakeRangeDownloader) DownloadRange(
	namespace, name string, dst io.Writer, offset, length int64) error {

	d.mu.Lock()
	d.calls++
	corrupt := d.failNext
	d.failNext = false
	delay := d.delay
	d.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	b := make([]byte, length)
	copy(b, d.content[offset:offset+length])
	if corrupt {
		for i := range b {
			b[i] ^= 0xff
		}
	}
	_, err := dst.Write(b)
	return err
}

func (d *fakeRangeDownloader) numCalls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func newPartialTorrent(
	t *testing.T,
	rd backend.RangeDownloader,
	blob *core.BlobFixture) (*Torrent, *store.CADownloadStore, func()) {

	cads, cleanup := store.CADownloadStoreFixture()
	tor, err := NewPartialTorrent(cads, rd, core.NamespaceFixture(), blob.MetaInfo)
	require.NoError(t, err)
	return tor, cads, cleanup
}

func readPiece(t *testing.T, tor *Torrent, pi int) []byte {
	r, err := tor.GetPieceReader(pi)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	return b
}

func TestPartialTorrentFetchesLazily(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(7, 2)
	rd := &fakeRangeDownloader{content: blob.Content}

	tor, _, cleanup := newPartialTorrent(t, rd, blob)
	defer cleanup()

	require.True(tor.Complete())
	require.Equal(blob.MetaInfo.InfoHash(), tor.InfoHash())

	require.Equal(blob.Content[0:2], readPiece(t, tor, 0))
	require.Equal(1, rd.numCalls())

	require.Equal(blob.Content[0:2], readPiece(t, tor, 0))
	require.Equal(1, rd.numCalls())

	require.Equal(blob.Content[2:4], readPiece(t, tor, 1))
	require.Equal(2, rd.numCalls())
}

func TestPartialTorrentShortLastPiece(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(7, 2)
	rd := &fakeRangeDownloader{content: blob.Content}

	tor, _, cleanup := newPartialTorrent(t, rd, blob)
	defer cleanup()

	last := tor.NumPieces() - 1
	require.Equal(int64(1), tor.PieceLength(last))

	require.Equal(blob.Content[6:7], readPiece(t, tor, last))
	require.Equal(1, rd.numCalls())
}

func TestPartialTorrentSumMismatchRefetchable(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(7, 2)
	rd := &fakeRangeDownloader{content: blob.Content, failNext: true}

	tor, _, cleanup := newPartialTorrent(t, rd, blob)
	defer cleanup()

	_, err := tor.GetPieceReader(0)
	require.Error(err)
	require.Equal(1, rd.numCalls())

	require.Equal(blob.Content[0:2], readPiece(t, tor, 0))
	require.Equal(2, rd.numCalls())
}

func TestPartialTorrentConcurrentSingleFetch(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(7, 2)
	rd := &fakeRangeDownloader{content: blob.Content, delay: 100 * time.Millisecond}

	tor, _, cleanup := newPartialTorrent(t, rd, blob)
	defer cleanup()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.Equal(blob.Content[0:2], readPiece(t, tor, 0))
		}()
	}
	wg.Wait()

	require.Equal(1, rd.numCalls())
}

func TestPartialTorrentRestartDurability(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(7, 2)
	rd := &fakeRangeDownloader{content: blob.Content}

	tor, cads, cleanup := newPartialTorrent(t, rd, blob)
	defer cleanup()

	require.Equal(blob.Content[0:2], readPiece(t, tor, 0))
	require.Equal(1, rd.numCalls())

	tor2, err := NewPartialTorrent(cads, rd, core.NamespaceFixture(), blob.MetaInfo)
	require.NoError(err)

	require.Equal(blob.Content[0:2], readPiece(t, tor2, 0))
	require.Equal(1, rd.numCalls())
}
