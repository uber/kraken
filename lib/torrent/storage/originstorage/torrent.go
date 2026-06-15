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
	cads      *store.CADownloadStore
	rd        backend.RangeDownloader
	namespace string
	pieces    []*piece
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
// from the backend into a sparse download file.
func NewPartialTorrent(
	cads *store.CADownloadStore,
	rd backend.RangeDownloader,
	namespace string,
	mi *core.MetaInfo) (*Torrent, error) {

	if err := cads.CreateDownloadFile(mi.Digest().Hex(), mi.Length()); err != nil &&
		!cads.InDownloadError(err) && !cads.InCacheError(err) {
		return nil, fmt.Errorf("create download file: %s", err)
	}
	pieces, _, err := restorePieces(mi.Digest(), cads, mi.NumPieces())
	if err != nil {
		return nil, fmt.Errorf("restore pieces: %s", err)
	}
	return &Torrent{
		metaInfo:    mi,
		numComplete: atomic.NewInt32(int32(mi.NumPieces())),
		partial:     true,
		cads:        cads,
		rd:          rd,
		namespace:   namespace,
		pieces:      pieces,
	}, nil
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
	return o.torrent.cads.Any().GetFileReader(o.torrent.Digest().Hex())
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
		return piecereader.NewFileReader(
			t.getFileOffset(pi), t.PieceLength(pi), &downloadOpener{t}), nil
	}
	return piecereader.NewFileReader(t.getFileOffset(pi), t.PieceLength(pi), &opener{t}), nil
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
	f, err := t.cads.GetDownloadFileReadWriter(t.metaInfo.Digest().Hex())
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
	if _, err := t.cads.Download().SetMetadataAt(
		t.Digest().Hex(), &pieceStatusMetadata{}, []byte{byte(_complete)}, int64(pi)); err != nil {
		return fmt.Errorf("write piece metadata: %s", err)
	}
	t.pieces[pi].markComplete()
	return nil
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

// getFileOffset calculates the offset in the torrent file given piece index.
// Assumes pi is a valid piece index.
func (t *Torrent) getFileOffset(pi int) int64 {
	return t.metaInfo.PieceLength() * int64(pi)
}
