package metainfogen

import (
	"github.com/uber/kraken/lib/store"

	"github.com/c2h5oh/datasize"
)

// Fixture returns a Generator which creates all metainfo with pieceLength for
// testing purposes.
func Fixture(cas *store.CAStore, pieceLength int) *Generator {
	g, err := New(Config{
		PieceLengths: map[datasize.ByteSize]datasize.ByteSize{0: datasize.ByteSize(pieceLength)},
	}, cas)
	if err != nil {
		panic(err)
	}
	return g
}
