package store

import (
	"regexp"

	"code.uber.internal/infra/kraken/lib/store/internal"
)

const torrentMetaSuffix = "_torrentmeta"

func init() {
	internal.RegisterMetadata(regexp.MustCompile(torrentMetaSuffix), &torrentMetaFactory{})
}

type torrentMetaFactory struct{}

func (f torrentMetaFactory) Create(suffix string) internal.MetadataType {
	return NewTorrentMeta()
}

type torrentMeta struct{}

// NewTorrentMeta return a new torrentMeat object
func NewTorrentMeta() internal.MetadataType {
	return &torrentMeta{}
}

func (m torrentMeta) GetSuffix() string {
	return torrentMetaSuffix
}

func (m torrentMeta) Movable() bool {
	return true
}
