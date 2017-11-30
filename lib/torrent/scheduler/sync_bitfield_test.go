package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncBitfieldDuplicateSetDoesNotDoubleCount(t *testing.T) {
	require := require.New(t)

	b := newSyncBitfield([]bool{false, false})
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
	b := newSyncBitfield([]bool{true, true, true})
	require.True(t, b.Complete())
}
