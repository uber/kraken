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
	"fmt"
	"io"
	"time"

	"github.com/andres-erbsen/clock"

	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/utils/closers"
)

// streamPollInterval is how long a streamReader waits between checks for the
// next piece to become available.
const streamPollInterval = 5 * time.Millisecond

// streamReadahead is how many pieces past the one a sequential read blocks on
// are demanded together, so the lazy dispatcher fetches ahead instead of one
// piece per poll.
const streamReadahead = 8

// streamReader serves a torrent's bytes while it is still downloading, blocking
// only on the piece needed for the current read rather than on the whole blob.
// It shares the dispatcher's live torrent instance, so HasPiece reflects pieces
// as they land.
//
// It implements store.FileReader (io.Reader, io.ReaderAt, io.Seeker, io.Closer,
// Size), so it can back the docker registry read path where http.ServeContent
// seeks and ranges over a blob that is still streaming in. Read/Seek are
// stateful and not safe for concurrent use; ReadAt is independent of the
// Read/Seek cursor.
type streamReader struct {
	t            storage.Torrent
	errc         chan error
	clk          clock.Clock
	pollInterval time.Duration
	// priority, if non-nil, hints the dispatcher to fetch a piece next when a
	// read blocks on it. Enables random-access (range) reads to skip ahead of
	// the in-order fetch.
	priority func(piece int)
	// request, if non-nil, demands a set of pieces from the dispatcher in lazy
	// mode, so only pieces a reader touches (plus readahead) are downloaded.
	request func(pieces []int)

	length   int64
	pieceLen int64 // Standard piece stride (PieceLength(0)); 0 for empty blobs.

	pos    int64               // Absolute position of the next sequential Read.
	pr     storage.PieceReader // Reader for the currently open piece, if any.
	prOff  int64               // Absolute position pr is positioned at.
	hinted int                 // Last piece hinted via priority (-1 if none).

	done    bool  // Terminal state received from errc.
	termErr error // Non-nil terminal download error.
}

func newStreamReader(
	t storage.Torrent,
	errc chan error,
	clk clock.Clock,
	pollInterval time.Duration,
	priority func(piece int),
	request func(pieces []int)) *streamReader {

	var pieceLen int64
	if t.NumPieces() > 0 {
		pieceLen = t.PieceLength(0)
	}
	return &streamReader{
		t:            t,
		errc:         errc,
		clk:          clk,
		pollInterval: pollInterval,
		priority:     priority,
		request:      request,
		length:       t.Length(),
		pieceLen:     pieceLen,
		hinted:       -1,
	}
}

// Size returns the total blob length, known from metainfo before download.
func (r *streamReader) Size() int64 {
	return r.length
}

func (r *streamReader) Read(p []byte) (int, error) {
	for {
		if r.pos >= r.length {
			return 0, io.EOF
		}
		if r.pr == nil || r.prOff != r.pos {
			if r.pr != nil {
				closers.Close(r.pr)
				r.pr = nil
			}
			if err := r.openAt(r.pos); err != nil {
				return 0, err
			}
		}
		n, err := r.pr.Read(p)
		if n > 0 {
			r.pos += int64(n)
			r.prOff += int64(n)
		}
		if err == io.EOF {
			closers.Close(r.pr)
			r.pr = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		if err != nil {
			return n, err
		}
		return n, nil
	}
}

// Seek moves the sequential Read cursor. The piece backing the new position is
// opened lazily on the next Read.
func (r *streamReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		abs = r.length + offset
	default:
		return 0, fmt.Errorf("stream reader: invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("stream reader: negative position %d", abs)
	}
	r.pos = abs
	return abs, nil
}

// ReadAt reads len(p) bytes at off, spanning pieces and blocking on each as it
// streams in. It does not touch the Read/Seek cursor.
func (r *streamReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("stream reader: negative offset %d", off)
	}
	if end := off + int64(len(p)); end > 0 && r.pieceLen > 0 {
		if end > r.length {
			end = r.length
		}
		r.demand(int(off/r.pieceLen), int((end-1)/r.pieceLen)+1)
	}
	var read int
	for read < len(p) {
		pos := off + int64(read)
		if pos >= r.length {
			return read, io.EOF
		}
		piece := int(pos / r.pieceLen)
		intra := pos - int64(piece)*r.pieceLen
		pr, err := r.acquirePiece(piece)
		if err != nil {
			return read, err
		}
		if intra > 0 {
			if _, err := io.CopyN(io.Discard, pr, intra); err != nil {
				closers.Close(pr)
				return read, err
			}
		}
		want := len(p) - read
		if rem := r.t.PieceLength(piece) - intra; int64(want) > rem {
			want = int(rem)
		}
		n, err := io.ReadFull(pr, p[read:read+want])
		closers.Close(pr)
		read += n
		if err != nil {
			return read, err
		}
	}
	return read, nil
}

// openAt opens a piece reader positioned at absolute offset pos.
func (r *streamReader) openAt(pos int64) error {
	piece := int(pos / r.pieceLen)
	intra := pos - int64(piece)*r.pieceLen
	pr, err := r.acquirePiece(piece)
	if err != nil {
		return err
	}
	if intra > 0 {
		if _, err := io.CopyN(io.Discard, pr, intra); err != nil {
			closers.Close(pr)
			return err
		}
	}
	r.pr = pr
	r.prOff = pos
	return nil
}

// acquirePiece blocks until the given piece is available (returning a reader for
// it) or the download reaches a terminal error state.
func (r *streamReader) acquirePiece(piece int) (storage.PieceReader, error) {
	for {
		if r.t.HasPiece(piece) {
			pr, err := r.t.GetPieceReader(piece)
			if err != nil {
				if werr := r.waitPiece(); werr != nil {
					return nil, werr
				}
				continue
			}
			return pr, nil
		}
		if r.hinted != piece {
			if r.priority != nil {
				r.priority(piece)
			}
			r.demand(piece, piece+streamReadahead)
			r.hinted = piece
		}
		if err := r.waitPiece(); err != nil {
			return nil, err
		}
	}
}

// demand asks the dispatcher (lazy mode) to fetch pieces [lo, hi), clamped to
// the torrent. No-op when request is nil (eager mode). Demand in the dispatcher
// is monotonic, so repeated overlapping calls are harmless.
func (r *streamReader) demand(lo, hi int) {
	if r.request == nil {
		return
	}
	if lo < 0 {
		lo = 0
	}
	if hi > r.t.NumPieces() {
		hi = r.t.NumPieces()
	}
	if lo >= hi {
		return
	}
	pieces := make([]int, 0, hi-lo)
	for i := lo; i < hi; i++ {
		pieces = append(pieces, i)
	}
	r.request(pieces)
}

// waitPiece blocks until either the poll interval elapses (progress may have
// been made) or the torrent reaches a terminal state. It returns a non-nil
// error only on a terminal download error.
func (r *streamReader) waitPiece() error {
	if r.done {
		return r.termErr
	}
	select {
	case err := <-r.errc:
		r.done = true
		r.termErr = err
		return err
	case <-r.clk.After(r.pollInterval):
		return nil
	}
}

func (r *streamReader) Close() error {
	if r.pr != nil {
		err := r.pr.Close()
		r.pr = nil
		return err
	}
	return nil
}
