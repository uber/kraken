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
package dispatch

import (
	"sync"
	"time"

	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/andres-erbsen/clock"
)

// torrentAccessWatcher wraps a storage.Torrent and records when it is written to
// and when it is read from. Read times are measured when piece readers are closed.
type torrentAccessWatcher struct {
	storage.Torrent
	clk       clock.Clock
	mu        sync.Mutex
	lastWrite time.Time
	lastRead  time.Time
}

func newTorrentAccessWatcher(t storage.Torrent, clk clock.Clock) *torrentAccessWatcher {
	return &torrentAccessWatcher{
		Torrent:   t,
		clk:       clk,
		lastWrite: clk.Now(),
		lastRead:  clk.Now(),
	}
}

func (w *torrentAccessWatcher) WritePiece(src storage.PieceReader, piece int) error {
	err := w.Torrent.WritePiece(src, piece)
	if err == nil {
		w.touchLastWrite()
	}
	return err
}

type pieceReaderCloseWatcher struct {
	storage.PieceReader
	w *torrentAccessWatcher
}

func (w *pieceReaderCloseWatcher) Close() error {
	err := w.PieceReader.Close()
	if err != nil {
		w.w.touchLastRead()
	}
	return err
}

func (w *torrentAccessWatcher) GetPieceReader(piece int) (storage.PieceReader, error) {
	pr, err := w.Torrent.GetPieceReader(piece)
	if err == nil {
		pr = &pieceReaderCloseWatcher{pr, w}
	}
	return pr, err
}

func (w *torrentAccessWatcher) touchLastWrite() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastWrite = w.clk.Now()
}

func (w *torrentAccessWatcher) touchLastRead() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastRead = w.clk.Now()
}

func (w *torrentAccessWatcher) getLastReadTime() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastRead
}

func (w *torrentAccessWatcher) getLastWriteTime() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastWrite
}
