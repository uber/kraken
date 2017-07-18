package store

import (
	"regexp"

	"code.uber.internal/infra/kraken/client/store/base"
)

const torrentMetaSuffix = "_torrentmeta"

func init() {
	base.RegisterMetadata(regexp.MustCompile(torrentMetaSuffix), &torrentMetaFactory{})
}

type torrentMetaFactory struct{}

func (f torrentMetaFactory) Create(suffix string) base.MetadataType {
	return NewTorrentMeta()
}

type torrentMeta struct{}

// NewTorrentMeta return a new torrentMeat object
func NewTorrentMeta() base.MetadataType {
	return &torrentMeta{}
}

func (m torrentMeta) GetSuffix() string {
	return torrentMetaSuffix
}

func (m torrentMeta) Movable() bool {
	return true
}
