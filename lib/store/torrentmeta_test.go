package store

import (
	"testing"

	"code.uber.internal/infra/kraken/core"

	"github.com/stretchr/testify/require"
)

func TestTorrentMetaSerialization(t *testing.T) {
	require := require.New(t)

	tm := NewTorrentMeta(core.MetaInfoFixture())
	b, err := tm.Serialize()
	require.NoError(err)

	var result TorrentMeta
	require.NoError(result.Deserialize(b))
	require.Equal(tm.MetaInfo, result.MetaInfo)
}
