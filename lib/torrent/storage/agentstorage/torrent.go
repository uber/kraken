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
	"io"
	"os"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"
	"github.com/uber/kraken/utils/log"

	"github.com/willf/bitset"
	"go.uber.org/atomic"
)

var (
	errPieceNotComplete   = errors.New("piece not complete")
	errWritePieceConflict = errors.New("piece is already being written to")
)

// caDownloadStore defines the CADownloadStore methods which Torrent requires. Useful
// for testing purposes, where we need to mock certain methods.
type caDownloadStore interface {
	MoveDownloadFileToCache(name string) error
	GetDownloadFileReadWriter(name string) (store.FileReadWriter, error)
	Any() *store.CADownloadStoreScope
	Download() *store.CADownloadStoreScope
	InCacheError(error) bool
}

// Torrent implements a Torrent on top of an AgentFileStore.
// It Allows concurrent writes on distinct pieces, and concurrent reads on all
// pieces. Behavior is undefined if multiple Torrent instances are backed
// by the same file store and metainfo.
type Torrent struct {
	metaInfo    *core.MetaInfo
	cads        caDownloadStore
	pieces      []*piece
	numComplete *atomic.Int32
	committed   *atomic.Bool
}

// NewTorrent creates a new Torrent.
func NewTorrent(cads caDownloadStore, mi *core.MetaInfo) (*Torrent, error) {
	pieces, numComplete, err := restorePieces(mi.Digest(), cads, mi.NumPieces())
	if err != nil {
		return nil, fmt.Errorf("restore pieces: %s", err)
	}

	committed := false
	if numComplete == len(pieces) {
		if err := cads.MoveDownloadFileToCache(mi.Digest().Hex()); err != nil && !os.IsExist(err) {
			return nil, fmt.Errorf("move file to cache: %s", err)
		}
		committed = true
	}

	return &Torrent{
		cads:        cads,
		metaInfo:    mi,
		pieces:      pieces,
		numComplete: atomic.NewInt32(int32(numComplete)),
		committed:   atomic.NewBool(committed),
	}, nil
}

// Digest returns the digest of the target blob.
func (t *Torrent) Digest() core.Digest {
	return t.metaInfo.Digest()
}

// Stat returns the storage.TorrentInfo for t.
func (t *Torrent) Stat() *storage.TorrentInfo {
	return storage.NewTorrentInfo(t.metaInfo, t.Bitfield())
}

// InfoHash returns the torrent metainfo hash.
func (t *Torrent) InfoHash() core.InfoHash {
	return t.metaInfo.InfoHash()
}

// NumPieces returns the number of pieces in the torrent.
func (t *Torrent) NumPieces() int {
	return len(t.pieces)
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

// Complete indicates whether the torrent is complete or not. Completeness is
// defined by whether the torrent file has been committed to the cache directory.
func (t *Torrent) Complete() bool {
	return t.committed.Load()
}

// BytesDownloaded returns an estimate of the number of bytes downloaded in the
// torrent.
func (t *Torrent) BytesDownloaded() int64 {
	return min(int64(t.numComplete.Load())*t.metaInfo.PieceLength(), t.metaInfo.Length())
}

// Bitfield returns the bitfield of pieces where true denotes a complete piece
// and false denotes an incomplete piece.
func (t *Torrent) Bitfield() *bitset.BitSet {
	bitfield := bitset.New(uint(len(t.pieces)))
	for i, p := range t.pieces {
		if p.complete() {
			bitfield.Set(uint(i))
		}
	}
	return bitfield
}

func (t *Torrent) String() string {
	downloaded := int(float64(t.BytesDownloaded()) / float64(t.metaInfo.Length()) * 100)
	return fmt.Sprintf(
		"torrent(name=%s, hash=%s, downloaded=%d%%)",
		t.Digest().Hex(), t.InfoHash().Hex(), downloaded)
}

func (t *Torrent) getPiece(pi int) (*piece, error) {
	if pi >= len(t.pieces) {
		return nil, fmt.Errorf("invalid piece index %d: num pieces = %d", pi, len(t.pieces))
	}
	return t.pieces[pi], nil
}

// markPieceComplete must only be called once per piece.
func (t *Torrent) markPieceComplete(pi int) error {
	updated, err := t.cads.Download().SetMetadataAt(
		t.Digest().Hex(), &pieceStatusMetadata{}, []byte{byte(_complete)}, int64(pi))
	if err != nil {
		return fmt.Errorf("write piece metadata: %s", err)
	}
	if !updated {
		// This could mean there's another thread with a Torrent instance using
		// the same file as us.
		log.Errorf(
			"Invariant violation: piece marked complete twice: piece %d in %s", pi, t.Digest().Hex())
	}
	t.pieces[pi].markComplete()
	t.numComplete.Inc()
	return nil
}

// writePiece writes data to piece pi. If the write succeeds, marks the piece as completed.
func (t *Torrent) writePiece(src storage.PieceReader, pi int) error {
	f, err := t.cads.GetDownloadFileReadWriter(t.metaInfo.Digest().Hex())
	if err != nil {
		return fmt.Errorf("get download writer: %s", err)
	}
	defer f.Close()

	h := core.PieceHash()
	r := io.TeeReader(src, h) // Calculates piece sum as we write to file.

	if _, err := f.Seek(t.getFileOffset(pi), 0); err != nil {
		return fmt.Errorf("seek: %s", err)
	}
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("copy: %s", err)
	}
	if h.Sum32() != t.metaInfo.GetPieceSum(pi) {
		return errors.New("invalid piece sum")
	}

	if err := t.markPieceComplete(pi); err != nil {
		return fmt.Errorf("mark piece complete: %s", err)
	}
	return nil
}

// WritePiece writes data to piece pi.
func (t *Torrent) WritePiece(src storage.PieceReader, pi int) error {
	piece, err := t.getPiece(pi)
	if err != nil {
		return err
	}
	if int64(src.Length()) != t.PieceLength(pi) {
		return fmt.Errorf(
			"invalid piece length: expected %d, got %d", t.PieceLength(pi), src.Length())
	}

	// Exit quickly if the piece is not writable.
	if piece.complete() {
		return storage.ErrPieceComplete
	}
	if piece.dirty() {
		return errWritePieceConflict
	}

	dirty, complete := piece.tryMarkDirty()
	if dirty {
		return errWritePieceConflict
	} else if complete {
		return storage.ErrPieceComplete
	}

	// At this point, we've determined that the piece is not complete and ensured
	// we are the only thread which may write the piece. We do not block other
	// threads from checking if the piece is writable.

	if err := t.writePiece(src, pi); err != nil {
		// Allow other threads to write this piece since we mysteriously failed.
		piece.markEmpty()
		return fmt.Errorf("write piece: %s", err)
	}

	if int(t.numComplete.Load()) == len(t.pieces) {
		// Multiple threads may attempt to move the download file to cache, however
		// only one will succeed while the others will receive (and ignore) file exist
		// error.
		err := t.cads.MoveDownloadFileToCache(t.metaInfo.Digest().Hex())
		if err != nil && !os.IsExist(err) {
			return fmt.Errorf("download completed but failed to move file to cache directory: %s", err)
		}
		t.committed.Store(true)
	}

	return nil
}

type opener struct {
	torrent *Torrent
}

func (o *opener) Open() (store.FileReader, error) {
	return o.torrent.cads.Any().GetFileReader(o.torrent.Digest().Hex())
}

// GetPieceReader returns a reader for piece pi.
func (t *Torrent) GetPieceReader(pi int) (storage.PieceReader, error) {
	piece, err := t.getPiece(pi)
	if err != nil {
		return nil, err
	}
	if !piece.complete() {
		return nil, errPieceNotComplete
	}
	return piecereader.NewFileReader(t.getFileOffset(pi), t.PieceLength(pi), &opener{t}), nil
}

// HasPiece returns if piece pi is complete.
func (t *Torrent) HasPiece(pi int) bool {
	piece, err := t.getPiece(pi)
	if err != nil {
		return false
	}
	return piece.complete()
}

// MissingPieces returns the indeces of all missing pieces.
func (t *Torrent) MissingPieces() []int {
	var missing []int
	for i, p := range t.pieces {
		if !p.complete() {
			missing = append(missing, i)
		}
	}
	return missing
}

// getFileOffset calculates the offset in the torrent file given piece index.
// Assumes pi is a valid piece index.
func (t *Torrent) getFileOffset(pi int) int64 {
	return t.metaInfo.PieceLength() * int64(pi)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
