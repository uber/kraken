package hostlist

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAttachPortIfMissing(t *testing.T) {
	addrs, err := attachPortIfMissing([]string{"x", "y:5", "z"}, 7)
	require.NoError(t, err)
	require.Equal(t, []string{"x:7", "y:5", "z:7"}, addrs)
}

func TestAttachPortIfMissingError(t *testing.T) {
	_, err := attachPortIfMissing([]string{"a:b:c"}, 7)
	require.Error(t, err)
}

func TestFilter(t *testing.T) {
	require.Equal(t, []string{"a", "b"}, filter([]string{"a", "c", "b"}, "c"))
}
