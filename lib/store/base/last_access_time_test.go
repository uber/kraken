package base

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMarshalAndUnmarshalLastAccessTime(t *testing.T) {
	require := require.New(t)

	lat := time.Now().Add(-time.Hour)
	b := MarshalLastAccessTime(lat)

	newLat, err := UnmarshalLastAccessTime(b)
	require.NoError(err)
	require.Equal(lat.Truncate(time.Second), newLat)
}
