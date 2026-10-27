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
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/gen/go/proto/p2p"
	"github.com/uber/kraken/lib/torrent/storage/agentstorage"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"
	"github.com/uber/kraken/utils/bitsetutil"
)

// A successful close is the normal outcome for a served piece, so both the
// watcher's own reader and the dispatcher's send path have to advance the last
// read time used by the SeederTTI preemption.
func TestSuccessfulReadRecordsAccess(t *testing.T) {
	tests := []struct {
		name         string
		serve        func(t *testing.T, clk *clock.Mock, d *Dispatcher, w *torrentAccessWatcher)
		lastReadTime func(d *Dispatcher, w *torrentAccessWatcher) time.Time
	}{
		{
			name: "piece reader close",
			serve: func(t *testing.T, clk *clock.Mock, _ *Dispatcher, w *torrentAccessWatcher) {
				pr, err := w.GetPieceReader(0)
				require.NoError(t, err)
				require.NoError(t, pr.Close())
			},
			lastReadTime: func(_ *Dispatcher, w *torrentAccessWatcher) time.Time {
				return w.getLastReadTime()
			},
		},
		{
			name: "dispatcher served piece",
			serve: func(t *testing.T, clk *clock.Mock, d *Dispatcher, _ *torrentAccessWatcher) {
				messages := newMockMessages()
				p := newPeer(
					core.PeerIDFixture(), false, bitsetutil.FromBools(true), messages, clk, &peerStats{})
				d.handlePieceRequest(p, &p2p.PieceRequestMessage{Index: 0, Offset: 0, Length: 4})

				// conn.sendPiecePayload defers closing the reader it was given.
				require.Len(t, messages.sent, 1)
				require.Equal(t, p2p.Message_PIECE_PAYLOAD, messages.sent[0].Message.Type)
				require.NoError(t, messages.sent[0].Payload.Close())
			},
			lastReadTime: func(d *Dispatcher, _ *torrentAccessWatcher) time.Time {
				return d.LastReadTime()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := clock.NewMock()
			blob := core.SizedBlobFixture(4, 4)
			torrent, cleanup := agentstorage.TorrentFixture(blob.MetaInfo)
			defer cleanup()
			require.NoError(t, torrent.WritePiece(piecereader.NewBuffer(blob.Content), 0))

			d := testDispatcher(Config{}, clk, torrent)
			w := newTorrentAccessWatcher(torrent, clk)

			before := tt.lastReadTime(d, w)
			clk.Add(time.Minute)

			tt.serve(t, clk, d, w)

			require.Equal(t, before.Add(time.Minute), tt.lastReadTime(d, w))
		})
	}
}
