package dispatch

import (
	"testing"

	"github.com/uber/kraken/utils/bitsetutil"

	"github.com/stretchr/testify/require"
)

func TestSyncBitfieldDuplicateSetDoesNotDoubleCount(t *testing.T) {
	require := require.New(t)

	b := newSyncBitfield(bitsetutil.FromBools(false, false))
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

	b := newSyncBitfield(bitsetutil.FromBools(true, true, true))
	require.True(b.Complete())
}

func TestSyncBitfieldString(t *testing.T) {
	require := require.New(t)

	b := newSyncBitfield(bitsetutil.FromBools(true, false, true, false))
	require.Equal("1010", b.String())
}
