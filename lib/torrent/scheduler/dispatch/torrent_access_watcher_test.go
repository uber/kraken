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
package dispatch

import (
	"io"
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/gen/go/proto/p2p"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"
	"github.com/uber/kraken/utils/bitsetutil"
)

type stubPieceReader struct{}

func (stubPieceReader) Read(p []byte) (int, error) { return 0, io.EOF }
func (stubPieceReader) Close() error               { return nil }
func (stubPieceReader) Length() int                { return 0 }

// stubTorrent embeds the interface so it inherits every method the dispatcher
// does not touch, and overrides the piece reader accessors used here.
type stubTorrent struct {
	storage.Torrent
	pr     storage.PieceReader
	length int
}

func (t *stubTorrent) GetPieceReader(int) (storage.PieceReader, error) { return t.pr, nil }
func (t *stubTorrent) NumPieces() int                                  { return 1 }
func (t *stubTorrent) MaxPieceLength() int64                           { return int64(t.length) }
func (t *stubTorrent) PieceLength(int) int64                           { return int64(t.length) }
func (t *stubTorrent) Complete() bool                                  { return false }

// A successful close is the normal outcome for a served piece, so it has to
// advance the last read time used by SeederTTI preemption.
func TestTorrentAccessWatcherRecordsSuccessfulRead(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	w := newTorrentAccessWatcher(&stubTorrent{pr: stubPieceReader{}}, clk)
	before := w.getLastReadTime()

	clk.Add(time.Minute)

	pr, err := w.GetPieceReader(0)
	require.NoError(err)
	require.NoError(pr.Close())

	require.Equal(before.Add(time.Minute), w.getLastReadTime())
}

// Serving a requested piece opens a reader through the access watcher and the
// conn closes it after sending the payload. Simulate both halves and check that
// the dispatcher observes the read.
func TestDispatcherRecordsServedPieceRead(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	d := testDispatcher(Config{}, clk, &stubTorrent{
		pr:     piecereader.NewBuffer([]byte("abcd")),
		length: 4,
	})
	messages := newMockMessages()
	p := newPeer(
		core.PeerIDFixture(), false, bitsetutil.FromBools(true), messages, clk, &peerStats{})

	before := d.LastReadTime()
	clk.Add(time.Minute)

	d.handlePieceRequest(p, &p2p.PieceRequestMessage{Index: 0, Offset: 0, Length: 4})

	// conn.sendPiecePayload defers closing the reader it was given.
	var payload storage.PieceReader
	for _, msg := range messages.sent {
		if msg.Message.Type == p2p.Message_PIECE_PAYLOAD {
			payload = msg.Payload
		}
	}
	require.NotNil(payload)
	require.NoError(payload.Close())

	require.Equal(before.Add(time.Minute), d.LastReadTime())
}
