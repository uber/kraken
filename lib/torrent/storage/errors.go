package storage

import (
	"fmt"

	"code.uber.internal/infra/kraken/torlib"
)

// InfoHashMismatchError implements error and contains expected and actual torlib.InfoHash
// TODO (@evelynl): this seems to be a fairly common error
type InfoHashMismatchError struct {
	expected torlib.InfoHash
	actual   torlib.InfoHash
}

func (ie InfoHashMismatchError) Error() string {
	return fmt.Sprintf("InfoHash missmatch: expected %s, actual %s", ie.expected.HexString(), ie.actual.HexString())
}

// IsInfoHashMismatchError returns true if error type is InfoHashMismatchError
func IsInfoHashMismatchError(err error) bool {
	switch err.(type) {
	case InfoHashMismatchError:
		return true
	}
	return false
}

// ConflictedPieceWriteError implements error and contains torrent name and piece index
type ConflictedPieceWriteError struct {
	torrent string
	piece   int
}

func (ce ConflictedPieceWriteError) Error() string {
	return fmt.Sprintf("Another thread is writing to the same piece %d for torrent %s", ce.piece, ce.torrent)
}

// IsConflictedPieceWriteError returns true if error type is ConflictedPieceWriteError
func IsConflictedPieceWriteError(err error) bool {
	switch err.(type) {
	case ConflictedPieceWriteError:
		return true
	}
	return false
}
