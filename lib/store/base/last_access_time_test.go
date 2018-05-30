package base

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLastAccessTimeSerialization(t *testing.T) {
	require := require.New(t)

	lat := NewLastAccessTime(time.Now().Add(-time.Hour))
	b, err := lat.Serialize()
	require.NoError(err)

	var newLat LastAccessTime
	require.NoError(newLat.Deserialize(b))
	require.Equal(lat.Time.Unix(), newLat.Time.Unix())
}
