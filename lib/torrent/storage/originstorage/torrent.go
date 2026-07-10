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
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/log"

	"github.com/willf/bitset"
	"go.uber.org/atomic"
)

// Torrent errors.
var (
	ErrReadOnly = errors.New("read-only torrent is being written to")
)

// Partial-mode fetch coordination tunables.
const (
	_partialFetchPollInterval = 50 * time.Millisecond
	_partialFetchTimeout      = 2 * time.Minute

	// _defaultFetchConcurrency bounds how many pieces of a partial torrent are
	// range-fetched from the backend at once (direct reads plus prefetchAhead).
	_defaultFetchConcurrency = 8
)

// Torrent is a read-only storage.Torrent. It allows concurrent reads on all
// pieces.
//
// It has two modes. In warm mode the blob is fully present in the local cache
// (cas) and pieces are served directly. In partial (cold) mode the blob is not
// cached: pieces are lazily range-fetched from the backend on demand and
// written into a sparse download file, so origin->backend egress is limited to
// the pieces agents actually request. Reported completeness is "complete" in
// both modes so agents connect and request any piece on demand.
type Torrent struct {
	metaInfo    *core.MetaInfo
	cas         *store.CAStore
	numComplete *atomic.Int32

	// Partial (cold) mode fields. Unused (nil) in warm mode.
	partial   bool
	rd        backend.RangeDownloader
	namespace string
	pieces    []*piece
	fetchSem  chan struct{} // Bounds concurrent backend range-fetches.
}

// NewTorrent creates a new warm Torrent backed by a complete cache file.
func NewTorrent(cas *store.CAStore, mi *core.MetaInfo) (*Torrent, error) {
	return &Torrent{
		cas:         cas,
		metaInfo:    mi,
		numComplete: atomic.NewInt32(int32(mi.NumPieces())),
	}, nil
}

// NewPartialTorrent creates a cold Torrent that lazily range-fetches pieces
// from the backend into a sparse download file. fetchConcurrency bounds how
// many pieces are range-fetched from the backend at once; <=0 uses
// _defaultFetchConcurrency.
func NewPartialTorrent(
	cas *store.CAStore,
	rd backend.RangeDownloader,
	namespace string,
	mi *core.MetaInfo,
	fetchConcurrency int) (*Torrent, error) {

	if fetchConcurrency <= 0 {
		fetchConcurrency = _defaultFetchConcurrency
	}
	if err := cas.CreateDownloadFile(mi.Digest().Hex(), mi.Length()); err != nil &&
		!cas.InDownloadError(err) && !cas.InCacheError(err) {
		return nil, fmt.Errorf("create download file: %s", err)
	}
	pieces, numComplete, err := restorePieces(mi.Digest(), cas, mi.NumPieces())
	if err != nil {
		return nil, fmt.Errorf("restore pieces: %s", err)
	}
	t := &Torrent{
		metaInfo:    mi,
		cas:         cas,
		numComplete: atomic.NewInt32(int32(numComplete)),
		partial:     true,
		rd:          rd,
		namespace:   namespace,
		pieces:      pieces,
		fetchSem:    make(chan struct{}, fetchConcurrency),
	}
	if numComplete == mi.NumPieces() {
		// Every piece was already restored complete from a prior run --
		// promote to the warm cache now instead of waiting for another
		// fetch that will never come (ensurePiece short-circuits on
		// p.complete() and never reaches markPieceComplete's promotion
		// check below).
		t.maybePromoteToCache()
	}
	return t, nil
}

// Digest returns the digest of the target blob.
func (t *Torrent) Digest() core.Digest {
	return t.metaInfo.Digest()
}

// Stat returns the TorrentInfo for t.
func (t *Torrent) Stat() *storage.TorrentInfo {
	return storage.NewTorrentInfo(t.metaInfo, t.Bitfield())
}

// InfoHash returns the torrent metainfo hash.
func (t *Torrent) InfoHash() core.InfoHash {
	return t.metaInfo.InfoHash()
}

// NumPieces returns the number of pieces in the torrent.
func (t *Torrent) NumPieces() int {
	return t.metaInfo.NumPieces()
}

// Length returns the length of the target file.
func (t *Torrent) Length() int64 {
	return t.metaInfo.Length()
}

// PieceLength returns the length of piece pi.
func (t *Torrent) PieceLength(pi int) int64 {
	return t.metaInfo.GetPieceLength(pi)
}

// MaxPieceLength returns the longest piece length of the torrent.
func (t *Torrent) MaxPieceLength() int64 {
	return t.PieceLength(0)
}

// Complete is always true.
func (t *Torrent) Complete() bool {
	return true
}

// BytesDownloaded always returns the total number of bytes.
func (t *Torrent) BytesDownloaded() int64 {
	return t.metaInfo.Length()
}

// WritePiece returns error, since Torrent is read-only.
func (t *Torrent) WritePiece(src storage.PieceReader, pi int) error {
	return ErrReadOnly
}

// Bitfield always returns a completed bitfield.
func (t *Torrent) Bitfield() *bitset.BitSet {
	return bitset.New(uint(t.NumPieces())).Complement()
}

func (t *Torrent) String() string {
	downloaded := int(float64(t.BytesDownloaded()) / float64(t.metaInfo.Length()) * 100)
	return fmt.Sprintf("torrent(hash=%s, downloaded=%d%%)", t.InfoHash().Hex(), downloaded)
}

type opener struct {
	torrent *Torrent
}

func (o *opener) Open() (store.FileReader, error) {
	return o.torrent.cas.GetCacheFileReader(o.torrent.Digest().Hex())
}

type downloadOpener struct {
	torrent *Torrent
}

func (o *downloadOpener) Open() (store.FileReader, error) {
	r, err := o.torrent.cas.GetDownloadFileReader(o.torrent.Digest().Hex())
	if o.torrent.cas.InCacheError(err) {
		// The last piece completed and promoted the download file to cache
		// between GetPieceReader constructing this reader and it actually
		// being opened here -- the blob is now fully in cache, read from there.
		return o.torrent.cas.GetCacheFileReader(o.torrent.Digest().Hex())
	}
	return r, err
}

// GetPieceReader returns a reader for piece pi. In partial mode the piece is
// range-fetched from the backend if not already present locally.
func (t *Torrent) GetPieceReader(pi int) (storage.PieceReader, error) {
	if pi >= t.NumPieces() {
		return nil, fmt.Errorf("invalid piece index %d: num pieces = %d", pi, t.NumPieces())
	}
	if t.partial {
		if err := t.ensurePiece(pi); err != nil {
			return nil, fmt.Errorf("ensure piece %d: %s", pi, err)
		}
		t.prefetchAhead(pi)
		return piecereader.NewFileReader(
			t.getFileOffset(pi), t.PieceLength(pi), &downloadOpener{t}), nil
	}
	return piecereader.NewFileReader(t.getFileOffset(pi), t.PieceLength(pi), &opener{t}), nil
}

// prefetchAhead opportunistically fetches up to cap(t.fetchSem) pieces after
// pi in the background, bounded by the fetch-concurrency semaphore. Errors
// are not surfaced here -- whichever caller actually reads a prefetched piece
// calls ensurePiece itself via GetPieceReader and gets the real error; this
// is best-effort, not the read path. Best-effort dedup with tryMarkDirty
// (via ensurePiece) means a background prefetch racing a direct read for the
// same piece never double-fetches.
func (t *Torrent) prefetchAhead(pi int) {
	for i := pi + 1; i < pi+1+cap(t.fetchSem) && i < len(t.pieces); i++ {
		if t.pieces[i].complete() {
			continue
		}
		select {
		case t.fetchSem <- struct{}{}:
			go func(i int) {
				defer func() { <-t.fetchSem }()
				if err := t.ensurePiece(i); err != nil {
					log.With("hash", t.InfoHash(), "piece", i).
						Debugf("Error prefetching piece: %s", err)
				}
			}(i)
		default:
			return
		}
	}
}

// ensurePiece guarantees piece pi is present and verified in the download file.
// Concurrent requests for the same piece trigger exactly one backend fetch.
func (t *Torrent) ensurePiece(pi int) error {
	p := t.pieces[pi]
	if p.complete() {
		return nil
	}
	dirty, complete := p.tryMarkDirty()
	if complete {
		return nil
	}
	if dirty {
		return t.waitForPiece(p)
	}
	if err := t.fetchPiece(pi); err != nil {
		p.markEmpty()
		return err
	}
	return t.markPieceComplete(pi)
}

// waitForPiece blocks until another goroutine finishes fetching p, or fails.
func (t *Torrent) waitForPiece(p *piece) error {
	deadline := time.Now().Add(_partialFetchTimeout)
	for {
		switch p.snapshot() {
		case _complete:
			return nil
		case _empty:
			return errors.New("concurrent piece fetch failed")
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for concurrent piece fetch")
		}
		time.Sleep(_partialFetchPollInterval)
	}
}

// fetchPiece range-fetches piece pi from the backend into the download file and
// verifies its CRC32 against the metainfo piece sum.
func (t *Torrent) fetchPiece(pi int) error {
	f, err := t.cas.GetDownloadFileReadWriter(t.metaInfo.Digest().Hex())
	if err != nil {
		return fmt.Errorf("get download writer: %s", err)
	}
	defer closers.Close(f)

	if _, err := f.Seek(t.getFileOffset(pi), 0); err != nil {
		return fmt.Errorf("seek: %s", err)
	}
	h := core.PieceHash()
	if err := t.rd.DownloadRange(
		t.namespace, t.Digest().Hex(), io.MultiWriter(f, h),
		t.getFileOffset(pi), t.PieceLength(pi)); err != nil {
		return fmt.Errorf("download range: %s", err)
	}
	if h.Sum32() != t.metaInfo.GetPieceSum(pi) {
		return errors.New("invalid piece sum")
	}
	return nil
}

// markPieceComplete persists the completed status for piece pi.
func (t *Torrent) markPieceComplete(pi int) error {
	if _, err := t.cas.SetDownloadFileMetadataAt(
		t.Digest().Hex(), &pieceStatusMetadata{}, []byte{byte(_complete)}, int64(pi)); err != nil {
		return fmt.Errorf("write piece metadata: %s", err)
	}
	t.pieces[pi].markComplete()
	if t.numComplete.Inc() == int32(t.NumPieces()) {
		t.maybePromoteToCache()
	}
	return nil
}

// maybePromoteToCache moves a fully-downloaded partial torrent's download
// file into the warm cache so later reads are served directly from cas
// instead of round-tripping through the download store forever. Best-effort:
// a failure here doesn't fail the read that completed the torrent.
func (t *Torrent) maybePromoteToCache() {
	if err := t.cas.MoveDownloadFileToCache(t.Digest().Hex(), t.Digest().Hex()); err != nil {
		log.With("hash", t.InfoHash()).Errorf("Error promoting completed partial torrent to cache: %s", err)
	}
}

// HasPiece returns if piece pi is complete.
// For Torrent it's always true.
func (t *Torrent) HasPiece(pi int) bool {
	return true
}

// MissingPieces always returns empty list.
func (t *Torrent) MissingPieces() []int {
	return []int{}
}

// pieceRange returns the [lo, hi) piece indices covering [offset, offset+length).
func (t *Torrent) pieceRange(offset, length int64) (int, int) {
	pl := t.metaInfo.PieceLength()
	lo := int(offset / pl)
	hi := int((offset+length-1)/pl) + 1
	return lo, hi
}

// ReadableRange implements storage.RangeReadable: whether every piece
// covering [offset, offset+length) has already landed locally, without
// touching HasPiece/Bitfield's always-complete lie (needed so agents can
// connect and request pieces on demand even before anything has landed).
// Warm torrents are always readable everywhere.
func (t *Torrent) ReadableRange(offset, length int64) bool {
	if !t.partial {
		return true
	}
	lo, hi := t.pieceRange(offset, length)
	if lo < 0 || hi > len(t.pieces) {
		return false
	}
	for pi := lo; pi < hi; pi++ {
		if !t.pieces[pi].complete() {
			return false
		}
	}
	return true
}

// CopyRange copies [offset, offset+length) into w, range-fetching any
// missing pieces on demand via GetPieceReader (blocking on ensurePiece's
// per-piece dedup/fetch, same as a P2P piece request would).
func (t *Torrent) CopyRange(w io.Writer, offset, length int64) error {
	lo, hi := t.pieceRange(offset, length)
	pos, remaining := offset, length
	for pi := lo; pi < hi && remaining > 0; pi++ {
		pr, err := t.GetPieceReader(pi)
		if err != nil {
			return fmt.Errorf("get piece reader %d: %s", pi, err)
		}
		skip := pos - int64(pi)*t.metaInfo.PieceLength()
		if skip > 0 {
			if _, err := io.CopyN(io.Discard, pr, skip); err != nil {
				closers.Close(pr)
				return fmt.Errorf("skip to offset: %s", err)
			}
		}
		want := t.PieceLength(pi) - skip
		if want > remaining {
			want = remaining
		}
		n, err := io.CopyN(w, pr, want)
		closers.Close(pr)
		pos += n
		remaining -= n
		if err != nil {
			return fmt.Errorf("copy piece %d: %s", pi, err)
		}
	}
	return nil
}

// getFileOffset calculates the offset in the torrent file given piece index.
// Assumes pi is a valid piece index.
func (t *Torrent) getFileOffset(pi int) int64 {
	return t.metaInfo.PieceLength() * int64(pi)
}
