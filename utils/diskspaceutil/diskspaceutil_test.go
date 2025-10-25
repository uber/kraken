package diskspaceutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/utils/diskspaceutil"
)

func TestFileSystemUtil(t *testing.T) {
	require := require.New(t)
	fsUtil, err := diskspaceutil.FileSystemUtil()
	require.NoError(err)

	require.Equal(true, fsUtil > 0)
	require.Equal(true, fsUtil < 100)
}

func TestFileSystemSize(t *testing.T) {
	require := require.New(t)
	fsSize, err := diskspaceutil.FileSystemSize()
	require.NoError(err)

	require.Equal(true, fsSize > 0)
}
