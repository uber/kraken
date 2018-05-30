package dockerregistry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStartedAtMetadataSerialization(t *testing.T) {
	require := require.New(t)

	s := newStartedAtMetadata(time.Now())
	b, err := s.Serialize()
	require.NoError(err)

	var result startedAtMetadata
	require.NoError(result.Deserialize(b))
	require.Equal(s.time.Unix(), result.time.Unix())
}

func TestHashState(t *testing.T) {
	require := require.New(t)

	h := newHashStateMetadata("sha256", "500")
	require.Equal(h.GetSuffix(), "_hashstates/sha256/500")
}
