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
	"io"
	"time"

	"github.com/andres-erbsen/clock"

	"github.com/uber/kraken/lib/torrent/storage"
)

// streamPollInterval is how long a streamReader waits between checks for the
// next piece to become available.
const streamPollInterval = 5 * time.Millisecond

// streamReader serves a torrent's bytes in piece order, blocking only on the
// piece needed for the current read rather than on the whole blob. It shares
// the dispatcher's live torrent instance, so HasPiece reflects pieces as they
// land. It is not safe for concurrent use (io.Copy reads sequentially).
type streamReader struct {
	t            storage.Torrent
	errc         chan error
	clk          clock.Clock
	pollInterval time.Duration

	numPieces int
	piece     int                 // Index of the next piece to read.
	pr        storage.PieceReader // Reader for the current piece, if open.
	done      bool                // Terminal state received from errc.
	termErr   error               // Non-nil terminal download error.
}

func newStreamReader(
	t storage.Torrent,
	errc chan error,
	clk clock.Clock,
	pollInterval time.Duration) *streamReader {

	return &streamReader{
		t:            t,
		errc:         errc,
		clk:          clk,
		pollInterval: pollInterval,
		numPieces:    t.NumPieces(),
	}
}

func (r *streamReader) Read(p []byte) (int, error) {
	for {
		if r.pr != nil {
			n, err := r.pr.Read(p)
			if err == io.EOF {
				closeErr := r.pr.Close()
				r.pr = nil
				r.piece++
				if n > 0 {
					return n, nil
				}
				if closeErr != nil {
					return 0, closeErr
				}
				continue
			}
			return n, err
		}
		if r.piece >= r.numPieces {
			return 0, io.EOF
		}
		if r.t.HasPiece(r.piece) {
			pr, err := r.t.GetPieceReader(r.piece)
			if err != nil {
				// Transient not-complete race; wait briefly and retry.
				if werr := r.waitPiece(); werr != nil {
					return 0, werr
				}
				continue
			}
			r.pr = pr
			continue
		}
		if err := r.waitPiece(); err != nil {
			return 0, err
		}
	}
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
