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
package metadata

import (
	"regexp"

	"github.com/uber/kraken/core"
)

const _torrentMetaSuffix = "_torrentmeta"

func init() {
	Register(regexp.MustCompile(_torrentMetaSuffix), &torrentMetaFactory{})
}

type torrentMetaFactory struct{}

func (f torrentMetaFactory) Create(suffix string) Metadata {
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
