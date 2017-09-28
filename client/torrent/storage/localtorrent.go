package storage

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"os"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"
)

// LocalTorrent implements Torrent
// Treat this as a wrapper on top of store.LocalStore
// It allows simultaneous read/write to different (or same, only for read) locations in the file
// TODO (@evelynl): add in-memory bookkeeping for file metadata in storage
type LocalTorrent struct {
	metaInfo *torlib.MetaInfo
	store    *store.LocalStore
}

// NewLocalTorrent creates a new LocalTorrent
func NewLocalTorrent(store *store.LocalStore, mi *torlib.MetaInfo) Torrent {
	return &LocalTorrent{
		store:    store,
		metaInfo: mi,
	}
}

// Name implements Torrent.Name
func (t *LocalTorrent) Name() string {
	return t.metaInfo.Info.Name
}

// InfoHash implements Torrent.InfoHash
func (t *LocalTorrent) InfoHash() torlib.InfoHash {
	return t.metaInfo.InfoHash
}

// NumPieces implements Torrent.NumPieces
func (t *LocalTorrent) NumPieces() int {
	return t.metaInfo.Info.NumPieces()
}

// Length implements Torrent.Length
func (t *LocalTorrent) Length() int64 {
	return t.metaInfo.Info.Length
}

// PieceLength implements Torrent.PieceLength
func (t *LocalTorrent) PieceLength(piece int) int64 {
	if piece == t.NumPieces()-1 {
		// Last piece
		return t.Length() - t.metaInfo.Info.PieceLength*int64(piece)
	}
	return t.metaInfo.Info.PieceLength
}

// MaxPieceLength implements Torrent.MaxPieceLength
func (t *LocalTorrent) MaxPieceLength() int64 {
	return t.PieceLength(0)
}

// Complete implements Torrent.Complete
func (t *LocalTorrent) Complete() bool {
	numPieces := t.NumPieces()

	// Read statuses of all pieces
	statuses, err := t.pieceStatuses()
	if err != nil {
		log.Error(err)
		return false
	}

	expected := make([]byte, numPieces)
	for i := 0; i < numPieces; i++ {
		expected[i] = store.PieceDone
	}

	return bytes.Compare(expected, statuses) == 0
}

// BytesDownloaded implements Torrent.BytesDownloaded
func (t *LocalTorrent) BytesDownloaded() int64 {
	var sum int64

	// Read statuses of all pieces
	statuses, err := t.pieceStatuses()
	if err != nil {
		log.Error(err)
		return sum
	}

	// Check status
	for p, status := range statuses {
		if status == store.PieceDone {
			sum += t.PieceLength(p)
		}
	}
	return sum
}

// Bitfield implements Torrent.Bitfield
func (t *LocalTorrent) Bitfield() Bitfield {
	bitfield := make([]bool, t.NumPieces())

	// Read statuses of all pieces
	statuses, err := t.pieceStatuses()
	if err != nil {
		log.Error(err)
		return bitfield
	}

	// Check status
	for p, status := range statuses {
		if status == store.PieceDone {
			bitfield[p] = true
		}
	}
	return bitfield
}

// String implements Torrent.String and returns a string representation of the torrent
func (t *LocalTorrent) String() string {
	return fmt.Sprintf("torrent(hash=%s, bitfield=%s)", t.InfoHash().HexString(), t.Bitfield())
}

// WritePiece implements Torrent.WritePieceAt
func (t *LocalTorrent) WritePiece(data []byte, piece int) (int, error) {
	verified := true
	err := t.verifyPiece(piece, data)
	if err != nil {
		verified = false
		log.Error(err)
	}

	fileOff, err := t.getFileOffset(piece, 0)
	if err != nil {
		return 0, err
	}

	// Set piece status
	err = t.preparePieceWrite(piece)
	if err != nil {
		return 0, err
	}

	// Get file writer
	writer, err := t.store.GetDownloadFileReadWriter(t.metaInfo.Info.Name)
	if err != nil {
		return 0, err
	}
	defer writer.Close()

	// Write piece
	n, err := writer.WriteAt(data, fileOff)
	if err != nil {
		verified = false
		return n, err
	}

	// Reset piece status
	if err := t.finishPieceWrite(piece, verified); err != nil {
		log.Error(err)
		return n, err
	}

	return n, nil
}

// ReadPiece implements Torrent.ReadPieceAt
func (t *LocalTorrent) ReadPiece(piece int) ([]byte, error) {
	data := make([]byte, t.PieceLength(piece))
	fileOff, err := t.getFileOffset(piece, 0)
	if err != nil {
		return nil, err
	}

	// Get file reader
	// It is ok if file is moved from download to cache
	reader, err := t.store.GetDownloadOrCacheFileReader(t.metaInfo.Info.Name)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	_, err = reader.ReadAt(data, fileOff)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// HasPiece implements Torrent.HasPiece
func (t *LocalTorrent) HasPiece(piece int) bool {
	status, err := t.store.GetFilePieceStatus(t.metaInfo.Info.Name, piece, 1)
	if err != nil {
		log.Error(err)
		return false
	}
	return status[0] == store.PieceDone
}

// MissingPieces implements Torrent.MissingPieces and returns the indexes of missing pieces
func (t *LocalTorrent) MissingPieces() []int {
	var missing []int

	// Read statuses of all pieces
	statuses, err := t.pieceStatuses()
	if err != nil {
		log.Error(err)
		return missing
	}

	// Check status
	for p, status := range statuses {
		if status == store.PieceDone {
			continue
		}
		missing = append(missing, p)
	}
	return missing
}

// verifyPiece verifies if the piece is complete
func (t *LocalTorrent) verifyPiece(piece int, data []byte) error {
	expectedHash, err := t.metaInfo.Info.PieceHash(piece)
	if err != nil {
		return err
	}

	h := sha1.New()
	h.Write(data)
	b := h.Sum(nil)

	if bytes.Compare(b, expectedHash) != 0 {
		return fmt.Errorf("Piece %d verification failed", piece)
	}
	return nil
}

// markPieceComplete implements Torrent.MarkPieceComplete
func (t *LocalTorrent) markPieceComplete(piece int) error {
	name := t.metaInfo.Info.Name
	_, err := t.store.WriteDownloadFilePieceStatusAt(name, []byte{store.PieceDone}, piece)
	if err != nil {
		return err
	}

	// On every call, check if all pieces are completed.
	// If so, try to move file from download to cache
	if t.Complete() {
		err = t.store.MoveDownloadFileToCache(name)
		if err != nil {
			if !os.IsExist(err) {
				log.Errorf("Download completed but failed to move file to cache directory: %s", err.Error())
				return err
			}
			return nil
		}
		log.Infof("Download completed and moved %s to cache directory", name)
	}
	return nil
}

// getFileOffset calculates the offset in the torrent file given piece index and offset according to the piece
func (t *LocalTorrent) getFileOffset(piece int, pieceOff int64) (int64, error) {
	off := t.metaInfo.Info.PieceLength*int64(piece) + pieceOff
	if off >= t.metaInfo.Info.Length {
		return -1, fmt.Errorf("Offset out of range: offset %d file length %d", off, t.metaInfo.Info.Length)
	}
	return off, nil
}

// preparePieceWrite marks the piece as being written
func (t *LocalTorrent) preparePieceWrite(piece int) error {
	name := t.metaInfo.Info.Name
	updated, err := t.store.WriteDownloadFilePieceStatusAt(name, []byte{store.PieceDirty}, piece)
	if err != nil {
		return err
	}

	// Another thread is writing to the same piece, current write should fail
	// TODO (@evelynl): verify that we clean the status file at restart, otherwise some pieces will be left in dirty state forever
	if !updated {
		return ConflictedPieceWriteError{name, piece}
	}
	return nil
}

// finishPieceWrite unmarks the piece and resumes other threads to write to this piece
func (t *LocalTorrent) finishPieceWrite(piece int, verified bool) error {
	name := t.metaInfo.Info.Name
	if verified {
		err := t.markPieceComplete(piece)
		if err == nil {
			return nil
		}
		log.Error(err)
	}
	updated, err := t.store.WriteDownloadFilePieceStatusAt(name, []byte{store.PieceClean}, piece)
	if err != nil {
		log.Error(err)
		return err
	}

	// Only the current thread should be able to unmark the piece
	// TODO (@evelynl): verify that we clean the status file at restart, otherwise some pieces will be left in dirty state forever
	if !updated {
		log.Errorf("Another thread is marking the same piece as clean. This should not happend. %s: %d", name, piece)
	}
	return nil
}

// pieceStatues returns status for all pieces
func (t *LocalTorrent) pieceStatuses() ([]byte, error) {
	return t.store.GetFilePieceStatus(t.metaInfo.Info.Name, 0, t.metaInfo.Info.NumPieces())
}
