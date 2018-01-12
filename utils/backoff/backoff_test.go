package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackoffAttempts(t *testing.T) {
	require := require.New(t)

	backoff := New(Config{
		Min:          250 * time.Millisecond,
		Max:          1 * time.Second,
		Factor:       2,
		NoJitter:     true,
		RetryTimeout: 2 * time.Second,
	})
	// Backoff should be:
	// 1st attempt: 0
	// 2nd attempt: 250ms
	// 3rd attempt: 500ms
	// 4th attempt: 1s
	var attempts int
	a := backoff.Attempts()
	for a.WaitForNext() {
		attempts++
	}
	require.Error(a.Err())
	require.Equal(4, attempts)
}

func TestBackoffAttemptsAlwaysExecutesOneAttemptRegardlessOfTimeout(t *testing.T) {
	require := require.New(t)

	// Timeout is  smaller than the min backoff, but we should still be able
	// to execute one attempt.
	backoff := New(Config{
		Min:          time.Second,
		RetryTimeout: 100 * time.Millisecond,
	})

	var attempts int
	a := backoff.Attempts()
	for a.WaitForNext() {
		attempts++
	}
	require.Error(a.Err())
	require.Equal(1, attempts)
}
