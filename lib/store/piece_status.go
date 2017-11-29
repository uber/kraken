package store

import (
	"regexp"

	"code.uber.internal/infra/kraken/lib/store/internal"
)

func init() {
	internal.RegisterMetadata(regexp.MustCompile("_status"), &pieceStatusFactory{})
}

type pieceStatusFactory struct{}

func (f pieceStatusFactory) Create(suffix string) internal.MetadataType {
	return NewPieceStatus()
}

type pieceStatus struct{}

// NewPieceStatus initializes and returns an new MetadataType obj.
func NewPieceStatus() internal.MetadataType {
	return pieceStatus{}
}

func (p pieceStatus) GetSuffix() string {
	return "_status"
}

func (p pieceStatus) Movable() bool {
	return true
}
