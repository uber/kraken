package store

import (
	"regexp"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store/base"
)

const _torrentMetaSuffix = "_torrentmeta"

func init() {
	base.RegisterMetadata(regexp.MustCompile(_torrentMetaSuffix), &torrentMetaFactory{})
}

type torrentMetaFactory struct{}

func (f torrentMetaFactory) Create(suffix string) base.Metadata {
	return &TorrentMeta{}
}

// TorrentMeta wraps torrent metainfo storage as metadata.
type TorrentMeta struct {
	MetaInfo *core.MetaInfo
}

// NewTorrentMeta return a new TorrentMeta.
func NewTorrentMeta(mi *core.MetaInfo) *TorrentMeta {
	return &TorrentMeta{mi}
}

// GetSuffix returns a static suffix.
func (m *TorrentMeta) GetSuffix() string {
	return _torrentMetaSuffix
}

// Movable is true.
func (m *TorrentMeta) Movable() bool {
	return true
}

// Serialize converts m to bytes.
func (m *TorrentMeta) Serialize() ([]byte, error) {
	return m.MetaInfo.Serialize()
}

// Deserialize loads b into m.
func (m *TorrentMeta) Deserialize(b []byte) error {
	mi, err := core.DeserializeMetaInfo(b)
	if err != nil {
		return err
	}
	m.MetaInfo = mi
	return nil
}
