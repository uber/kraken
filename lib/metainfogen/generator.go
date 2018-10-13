package metainfogen

import (
	"fmt"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/store/metadata"
)

// Generator wraps static piece length configuration in order to determinstically
// generate metainfo.
type Generator struct {
	pieceLengthConfig *pieceLengthConfig
	cas               *store.CAStore
}

// New creates a new Generator.
func New(config Config, cas *store.CAStore) (*Generator, error) {
	plConfig, err := newPieceLengthConfig(config.PieceLengths)
	if err != nil {
		return nil, fmt.Errorf("piece length config: %s", err)
	}
	return &Generator{plConfig, cas}, nil
}

// Generate generates metainfo for the blob of d and writes it to disk.
func (g *Generator) Generate(d core.Digest) error {
	info, err := g.cas.GetCacheFileStat(d.Hex())
	if err != nil {
		return fmt.Errorf("cache stat: %s", err)
	}
	f, err := g.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		return fmt.Errorf("get cache file: %s", err)
	}
	pieceLength := g.pieceLengthConfig.get(info.Size())
	mi, err := core.NewMetaInfo(d, f, pieceLength)
	if err != nil {
		return fmt.Errorf("create metainfo: %s", err)
	}
	if _, err := g.cas.SetCacheFileMetadata(d.Hex(), metadata.NewTorrentMeta(mi)); err != nil {
		return fmt.Errorf("set metainfo: %s", err)
	}
	return nil
}
