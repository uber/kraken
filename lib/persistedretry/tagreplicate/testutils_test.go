package tagreplicate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func EqualTask(t *testing.T, expected Task, result Task) {
	expected.stats = nil
	result.stats = nil

	require := require.New(t)
	require.Equal(expected.CreatedAt.Unix(), result.CreatedAt.Unix())
	expected.CreatedAt = time.Time{}
	result.CreatedAt = time.Time{}

	require.Equal(expected.LastAttempt.Unix(), result.LastAttempt.Unix())
	expected.LastAttempt = time.Time{}
	result.LastAttempt = time.Time{}

	require.Equal(expected, result)
}
