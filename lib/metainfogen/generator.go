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
package metainfogen

import (
	"fmt"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/lib/store/tiered"
)

// Generator wraps static piece length configuration in order to determinstically
// generate metainfo.
type Generator struct {
	pieceLengthConfig *pieceLengthConfig
	store             *tiered.Store
}

// New creates a new Generator.
func New(config Config, store *tiered.Store) (*Generator, error) {
	plConfig, err := newPieceLengthConfig(config.PieceLengths)
	if err != nil {
		return nil, fmt.Errorf("piece length config: %s", err)
	}
	return &Generator{plConfig, store.ScopeComplete()}, nil
}

// Generate generates metainfo for the blob of d and writes it to store.
func (g *Generator) Generate(d core.Digest) error {
	size, err := g.store.Stat(d.Hex())
	if err != nil {
		return fmt.Errorf("stat blob: %s", err)
	}
	f, err := g.store.Open(d.Hex())
	if err != nil {
		return fmt.Errorf("open blob: %s", err)
	}
	pieceLength := g.pieceLengthConfig.get(size)
	mi, err := core.NewMetaInfo(d, f, pieceLength)
	if err != nil {
		return fmt.Errorf("create metainfo: %s", err)
	}
	if err := g.store.SetMetadata(d.Hex(), metadata.NewTorrentMeta(mi)); err != nil {
		return fmt.Errorf("set blob metadata: %s", err)
	}
	return nil
}

// Get the piece length for the blob
func (g *Generator) GetPieceLength(size int64) int64 {
	return g.pieceLengthConfig.get(size)
}
