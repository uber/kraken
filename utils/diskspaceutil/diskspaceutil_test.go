package diskspaceutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/utils/diskspaceutil"
)

func TestUsage(t *testing.T) {
	require := require.New(t)
	usage, err := diskspaceutil.Usage()
	require.NoError(err)

	require.True(usage.TotalBytes > 0)
	require.True(usage.FreeBytes > 0)
	require.True(usage.UsedBytes > 0)
	require.True(usage.Util > 0)
}
