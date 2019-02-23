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
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"
	"github.com/uber/kraken/mocks/lib/store"
	"github.com/uber/kraken/utils/bitsetutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func prepareStore(cads *store.CADownloadStore, mi *core.MetaInfo) {
	if err := cads.CreateDownloadFile(mi.Digest().Hex(), mi.Length()); err != nil {
		panic(err)
	}
	if _, err := cads.Download().SetMetadata(mi.Digest().Hex(), metadata.NewTorrentMeta(mi)); err != nil {
		panic(err)
	}
}

func TestTorrentCreate(t *testing.T) {
	require := require.New(t)

	cads, cleanup := store.CADownloadStoreFixture()
	defer cleanup()

	mi := core.SizedBlobFixture(7, 2).MetaInfo

	prepareStore(cads, mi)

	tor, err := NewTorrent(cads, mi)
	require.NoError(err)

	// New torrent
	require.Equal(mi.Digest(), tor.Digest())
	require.Equal(4, tor.NumPieces())
	require.Equal(int64(7), tor.Length())
	require.Equal(int64(2), tor.PieceLength(0))
	require.Equal(int64(1), tor.PieceLength(3))
	require.Equal(mi.InfoHash(), tor.InfoHash())
	require.False(tor.Complete())
	require.Equal(int64(0), tor.BytesDownloaded())
	require.Equal(bitsetutil.FromBools(false, false, false, false), tor.Bitfield())
	require.False(tor.HasPiece(0))
	require.Equal([]int{0, 1, 2, 3}, tor.MissingPieces())
}

func TestTorrentWriteUpdatesBytesDownloadedAndBitfield(t *testing.T) {
	require := require.New(t)

	cads, cleanup := store.CADownloadStoreFixture()
	defer cleanup()

	blob := core.SizedBlobFixture(2, 1)

	prepareStore(cads, blob.MetaInfo)

	tor, err := NewTorrent(cads, blob.MetaInfo)
	require.NoError(err)

	require.NoError(tor.WritePiece(piecereader.NewBuffer(blob.Content[:1]), 0))
	require.False(tor.Complete())
	require.Equal(int64(1), tor.BytesDownloaded())
	require.Equal(bitsetutil.FromBools(true, false), tor.Bitfield())
}

func TestTorrentWriteComplete(t *testing.T) {
	require := require.New(t)

	cads, cleanup := store.CADownloadStoreFixture()
	defer cleanup()

	blob := core.SizedBlobFixture(1, 1)

	prepareStore(cads, blob.MetaInfo)

	tor, err := NewTorrent(cads, blob.MetaInfo)
	require.NoError(err)

	require.NoError(tor.WritePiece(piecereader.NewBuffer(blob.Content), 0))

	r, err := tor.GetPieceReader(0)
	require.NoError(err)
	defer r.Close()
	result, err := ioutil.ReadAll(r)
	require.NoError(err)
	require.Equal(blob.Content, result)

	require.True(tor.Complete())
	require.Equal(int64(1), tor.BytesDownloaded())

	// Duplicate write should detect piece is complete.
	require.Equal(storage.ErrPieceComplete, tor.WritePiece(piecereader.NewBuffer(blob.Content[:1]), 0))
}

func TestTorrentWriteMultiplePieceConcurrent(t *testing.T) {
	require := require.New(t)

	cads, cleanup := store.CADownloadStoreFixture()
	defer cleanup()

	blob := core.SizedBlobFixture(7, 2)

	prepareStore(cads, blob.MetaInfo)

	tor, err := NewTorrent(cads, blob.MetaInfo)
	require.NoError(err)

	wg := sync.WaitGroup{}
	wg.Add(tor.NumPieces())
	for i := 0; i < tor.NumPieces(); i++ {
		go func(i int) {
			defer wg.Done()
			start := i * int(blob.MetaInfo.PieceLength())
			end := start + int(tor.PieceLength(i))
			require.NoError(tor.WritePiece(piecereader.NewBuffer(blob.Content[start:end]), i))
		}(i)
	}

	wg.Wait()

	// Complete
	require.True(tor.Complete())
	require.Equal(int64(7), tor.BytesDownloaded())
	require.Nil(tor.MissingPieces())

	// Check content
	reader, err := cads.Cache().GetFileReader(blob.MetaInfo.Digest().Hex())
	require.NoError(err)
	torrentBytes, err := ioutil.ReadAll(reader)
	require.NoError(err)
	require.Equal(blob.Content, torrentBytes)
}

func TestTorrentWriteSamePieceConcurrent(t *testing.T) {
	require := require.New(t)

	cads, cleanup := store.CADownloadStoreFixture()
	defer cleanup()

	blob := core.SizedBlobFixture(16, 1)

	prepareStore(cads, blob.MetaInfo)

	tor, err := NewTorrent(cads, blob.MetaInfo)
	require.NoError(err)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			pi := int(math.Mod(float64(i), float64(len(blob.Content))))

			// If another goroutine is currently writing, we should get errWritePieceConflict.
			// If another goroutine has finished writing, we should get storage.ErrPieceComplete.
			err := tor.WritePiece(piecereader.NewBuffer([]byte{blob.Content[pi]}), pi)
			if err != nil {
				require.Contains([]error{errWritePieceConflict, storage.ErrPieceComplete}, err)
			}

			start := time.Now()
			timeout := time.Duration(100 * time.Millisecond)
			for {
				time.Sleep(5 * time.Millisecond)
				if time.Since(start) > timeout {
					require.FailNow(fmt.Sprintf("failed to get piece reader %v after writing", timeout))
				}

				// If another goroutine was writing when we tried to, we will get errPieceNotComplete
				// until they finish.
				r, err := tor.GetPieceReader(pi)
				if err != nil {
					require.Equal(errPieceNotComplete, err)
					continue
				}
				defer r.Close()

				result, err := ioutil.ReadAll(r)
				require.NoError(err)
				require.Equal(1, len(result))
				require.Equal(1, len(result))
				require.Equal(blob.Content[pi], result[0])

				return
			}
		}(i)
	}
	wg.Wait()

	reader, err := cads.Cache().GetFileReader(blob.MetaInfo.Digest().Hex())
	require.NoError(err)
	torrentBytes, err := ioutil.ReadAll(reader)
	require.NoError(err)
	require.Equal(blob.Content, torrentBytes)
}

// mockGetDownloadFileReadWriterStore wraps an internal CADownloadStore but
// overrides the GetDownloadFileReadWriter method to return f.
type mockGetDownloadFileReadWriterStore struct {
	*store.CADownloadStore
	f store.FileReadWriter
}

func (s *mockGetDownloadFileReadWriterStore) GetDownloadFileReadWriter(
	name string) (store.FileReadWriter, error) {

	return s.f, nil
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

func (w *coordinatedWriter) Write(b []byte) (int, error) {
	w.startWriting <- true
	<-w.stopWriting
	return len(b), nil
}

func TestTorrentWritePieceConflictsDoNotBlock(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(1, 1)

	f, cleanup := store.NewMockFileReadWriter([]byte{})
	defer cleanup()

	w := newCoordinatedWriter(f)

	cads, cleanup := store.CADownloadStoreFixture()
	defer cleanup()

	prepareStore(cads, blob.MetaInfo)

	mockCADS := &mockGetDownloadFileReadWriterStore{cads, w}

	tor, err := NewTorrent(mockCADS, blob.MetaInfo)
	require.NoError(err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		require.NoError(tor.WritePiece(piecereader.NewBuffer(blob.Content), 0))
	}()

	// Writing while another goroutine is mid-write should not block.
	<-w.startWriting
	require.Equal(errWritePieceConflict, tor.WritePiece(piecereader.NewBuffer(blob.Content), 0))
	w.stopWriting <- true

	<-done

	// Duplicate write should detect piece is complete.
	require.Equal(storage.ErrPieceComplete, tor.WritePiece(piecereader.NewBuffer(blob.Content), 0))
}

func TestTorrentWritePieceFailuresRemoveDirtyStatus(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := mockstore.NewMockFileReadWriter(ctrl)

	cads, cleanup := store.CADownloadStoreFixture()
	defer cleanup()

	blob := core.SizedBlobFixture(1, 1)

	prepareStore(cads, blob.MetaInfo)

	mockCADS := &mockGetDownloadFileReadWriterStore{cads, w}

	gomock.InOrder(
		// First write fails.
		w.EXPECT().Seek(int64(0), 0).Return(int64(0), nil),
		w.EXPECT().Write(blob.Content).Return(0, errors.New("first write error")),
		w.EXPECT().Close().Return(nil),

		// Second write succeeds.
		w.EXPECT().Seek(int64(0), 0).Return(int64(0), nil),
		w.EXPECT().Write(blob.Content).Return(len(blob.Content), nil),
		w.EXPECT().Close().Return(nil),
	)

	tor, err := NewTorrent(mockCADS, blob.MetaInfo)
	require.NoError(err)

	// After the first write fails, the dirty bit should be flipped to empty,
	// allowing future writes to succeed.
	require.Error(tor.WritePiece(piecereader.NewBuffer(blob.Content), 0))
	require.NoError(tor.WritePiece(piecereader.NewBuffer(blob.Content), 0))
}

func TestTorrentRestoreCompletedTorrent(t *testing.T) {
	require := require.New(t)

	cads, cleanup := store.CADownloadStoreFixture()
	defer cleanup()

	blob := core.SizedBlobFixture(8, 1)

	prepareStore(cads, blob.MetaInfo)

	tor, err := NewTorrent(cads, blob.MetaInfo)
	require.NoError(err)

	for i, b := range blob.Content {
		require.NoError(tor.WritePiece(piecereader.NewBuffer([]byte{b}), i))
	}

	require.True(tor.Complete())

	tor, err = NewTorrent(cads, blob.MetaInfo)
	require.NoError(err)

	require.True(tor.Complete())
}

func TestTorrentRestoreInProgressTorrent(t *testing.T) {
	require := require.New(t)

	cads, cleanup := store.CADownloadStoreFixture()
	defer cleanup()

	blob := core.SizedBlobFixture(8, 1)

	prepareStore(cads, blob.MetaInfo)

	tor, err := NewTorrent(cads, blob.MetaInfo)
	require.NoError(err)

	pi := 4

	require.NoError(tor.WritePiece(piecereader.NewBuffer([]byte{blob.Content[pi]}), pi))
	require.Equal(int64(1), tor.BytesDownloaded())

	tor, err = NewTorrent(cads, blob.MetaInfo)
	require.NoError(err)

	require.Equal(int64(1), tor.BytesDownloaded())
	require.Equal(
		storage.ErrPieceComplete,
		tor.WritePiece(piecereader.NewBuffer([]byte{blob.Content[pi]}), pi))
}
