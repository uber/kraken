package scheduler

import (
	"testing"

	"code.uber.internal/infra/kraken/lib/torrent/storage"

	"github.com/stretchr/testify/require"
)

func TestSyncBitfieldDuplicateSetDoesNotDoubleCount(t *testing.T) {
	require := require.New(t)

	b := newSyncBitfield(storage.BitSetFixture(false, false))
	require.False(b.Complete())

	b.Set(0, true)
	require.False(b.Complete())
	b.Set(0, true)
	require.False(b.Complete())

	b.Set(1, true)
	require.True(b.Complete())

	b.Set(1, false)
	require.False(b.Complete())
	b.Set(1, false)
	require.False(b.Complete())

	b.Set(1, true)
	require.True(b.Complete())
}

func TestSyncBitfieldNewCountsNumComplete(t *testing.T) {
	require := require.New(t)

	b := newSyncBitfield(storage.BitSetFixture(true, true, true))
	require.True(b.Complete())
}

func TestSyncBitfieldString(t *testing.T) {
	require := require.New(t)

	b := newSyncBitfield(storage.BitSetFixture(true, false, true, false))
	require.Equal("1010", b.String())
}
