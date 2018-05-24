package hostlist

import (
	"testing"

	"code.uber.internal/infra/kraken/utils/stringset"
	"github.com/stretchr/testify/require"
)

func TestAttachPortIfMissing(t *testing.T) {
	addrs, err := attachPortIfMissing(stringset.New("x", "y:5", "z"), 7)
	require.NoError(t, err)
	require.Equal(t, stringset.New("x:7", "y:5", "z:7"), addrs)
}

func TestAttachPortIfMissingError(t *testing.T) {
	_, err := attachPortIfMissing(stringset.New("a:b:c"), 7)
	require.Error(t, err)
}
