package diskspaceutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/utils/diskspaceutil"
)

func TestParseManifestV2List(t *testing.T) {
	util, err := diskspaceutil.DiskSpaceUtil()
	require.NoError(t, err)

	require.Equal(t, true, util > 0)
	require.Equal(t, true, util < 100)
}
