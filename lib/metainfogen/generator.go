package metainfogen

import (
	"fmt"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
)

// Generator wraps static piece length configuration in order to determinstically
// generate metainfo.
type Generator struct {
	pieceLengthConfig *pieceLengthConfig
	fs                store.OriginFileStore
}

// New creates a new Generator.
func New(config Config, fs store.OriginFileStore) (*Generator, error) {
	plConfig, err := newPieceLengthConfig(config.PieceLengths)
	if err != nil {
		return nil, fmt.Errorf("piece length config: %s", err)
	}
	return &Generator{plConfig, fs}, nil
}

// Generate generates metainfo for the blob of d and writes it to disk.
func (g *Generator) Generate(d core.Digest) error {
	info, err := g.fs.GetCacheFileStat(d.Hex())
	if err != nil {
		return fmt.Errorf("cache stat: %s", err)
	}
	f, err := g.fs.GetCacheFileReader(d.Hex())
	if err != nil {
		return fmt.Errorf("get cache file: %s", err)
	}
	pieceLength := g.pieceLengthConfig.get(info.Size())
	mi, err := core.NewMetaInfoFromBlob(d.Hex(), f, pieceLength)
	if err != nil {
		return fmt.Errorf("create metainfo: %s", err)
	}
	if _, err := g.fs.SetCacheFileMetadata(d.Hex(), store.NewTorrentMeta(mi)); err != nil {
		return fmt.Errorf("set metainfo: %s", err)
	}
	return nil
}
