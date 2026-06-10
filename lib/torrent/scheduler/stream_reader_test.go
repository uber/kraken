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
package scheduler

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/lib/torrent/storage/agentstorage"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"
)

func streamTorrentFixture(
	t *testing.T, blob *core.BlobFixture) (*agentstorage.Torrent, func()) {

	cads, cleanup := store.CADownloadStoreFixture()
	mi := blob.MetaInfo
	if err := cads.CreateDownloadFile(mi.Digest().Hex(), mi.Length()); err != nil {
		cleanup()
		t.Fatal(err)
	}
	if _, err := cads.Download().SetMetadata(
		mi.Digest().Hex(), metadata.NewTorrentMeta(mi)); err != nil {
		cleanup()
		t.Fatal(err)
	}
	tor, err := agentstorage.NewTorrent(cads, mi)
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	return tor, cleanup
}

func writePiece(t *testing.T, tor storage.Torrent, blob *core.BlobFixture, i int) {
	t.Helper()
	off := tor.PieceLength(0) * int64(i)
	end := off + tor.PieceLength(i)
	require.NoError(t, tor.WritePiece(piecereader.NewBuffer(blob.Content[off:end]), i))
}

func TestStreamReaderServesPiecesAsTheyArrive(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(100, 10) // 10 pieces.
	tor, cleanup := streamTorrentFixture(t, blob)
	defer cleanup()

	errc := make(chan error, 1)
	r := newStreamReader(tor, errc, clock.New(), time.Millisecond)
	t.Cleanup(func() { require.NoError(r.Close()) })

	// Write pieces in order from a separate goroutine, lagging the reader.
	go func() {
		for i := 0; i < tor.NumPieces(); i++ {
			time.Sleep(2 * time.Millisecond)
			writePiece(t, tor, blob, i)
		}
	}()

	out, err := io.ReadAll(r)
	require.NoError(err)
	require.Equal(blob.Content, out)
}

func TestStreamReaderHandlesAlreadyCompleteTorrent(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(50, 10)
	tor, cleanup := streamTorrentFixture(t, blob)
	defer cleanup()

	for i := 0; i < tor.NumPieces(); i++ {
		writePiece(t, tor, blob, i)
	}
	require.True(tor.Complete())

	errc := make(chan error, 1)
	errc <- nil
	r := newStreamReader(tor, errc, clock.New(), time.Millisecond)
	t.Cleanup(func() { require.NoError(r.Close()) })

	out, err := io.ReadAll(r)
	require.NoError(err)
	require.Equal(blob.Content, out)
}

func TestStreamReaderReturnsTerminalError(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(100, 10)
	tor, cleanup := streamTorrentFixture(t, blob)
	defer cleanup()

	// Only the first piece ever arrives.
	writePiece(t, tor, blob, 0)

	errc := make(chan error, 1)
	r := newStreamReader(tor, errc, clock.New(), time.Millisecond)
	t.Cleanup(func() { require.NoError(r.Close()) })

	// Signal a terminal download failure; the reader blocks on piece 1 and
	// should surface this error.
	downloadErr := errors.New("download failed")
	errc <- downloadErr

	out, err := io.ReadAll(r)
	require.Equal(downloadErr, err)
	require.Equal(blob.Content[:tor.PieceLength(0)], out)
}
