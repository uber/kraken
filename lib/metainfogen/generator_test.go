package metainfogen

import (
	"bytes"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/store/metadata"

	"github.com/andres-erbsen/clock"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
)

func TestGenerate(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.OriginFileStoreFixture(clock.New())
	defer cleanup()

	pieceLength := 10

	generator, err := New(Config{
		PieceLengths: map[datasize.ByteSize]datasize.ByteSize{
			0: datasize.ByteSize(pieceLength),
		},
	}, fs)
	require.NoError(err)

	blob := core.SizedBlobFixture(100, uint64(pieceLength))

	require.NoError(fs.CreateCacheFile(blob.Digest.Hex(), bytes.NewReader(blob.Content)))

	require.NoError(generator.Generate(blob.Digest))

	var tm metadata.TorrentMeta
	require.NoError(fs.GetCacheFileMetadata(blob.Digest.Hex(), &tm))
	require.Equal(blob.MetaInfo, tm.MetaInfo)
}
