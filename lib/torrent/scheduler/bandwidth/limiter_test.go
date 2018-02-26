package bandwidth

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimiterReserveEgressConcurrency(t *testing.T) {
	require := require.New(t)

	config := Config{
		EgressBitsPerSec: 800, // 100 bytes.
		TokenSize:        1,
	}
	l := NewLimiter(config)

	// This test starts a bunch of goroutines and see how many bytes they can
	// reserve in nsecs.
	nsecs := 4

	stop := make(chan struct{})
	go func() {
		<-time.After(time.Duration(nsecs) * time.Second)
		close(stop)
	}()

	var mu sync.Mutex
	var nbytes int

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				require.NoError(l.ReserveEgress(1))
				select {
				case <-stop:
					return
				default:
					mu.Lock()
					nbytes++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	// The bucket is initially full, hence nsecs + 1.
	require.InDelta(config.EgressBitsPerSec*uint64(nsecs+1), 8*nbytes, 10.0)
}

func TestLimiterReserveEgressBytesTokenScaling(t *testing.T) {
	require := require.New(t)

	config := Config{
		EgressBitsPerSec: 80, // 10 bytes.
		TokenSize:        10, // Bucket has 8 tokens.
	}
	l := NewLimiter(config)

	start := time.Now()
	// Reserving two buckets full of tokens should take exactly one second.
	for i := 0; i < 4; i++ {
		// 6 bytes -> 48 bits, which is should be equal to 4 tokens.
		require.NoError(l.ReserveEgress(6))
	}
	require.InDelta(time.Second, time.Since(start), float64(50*time.Millisecond))
}

func TestLimiterReserveEgressBytesSmallerThanTokenSize(t *testing.T) {
	require := require.New(t)

	config := Config{
		EgressBitsPerSec: 80, // 10 bytes.
		TokenSize:        10, // Bucket has 8 tokens.
	}
	l := NewLimiter(config)

	start := time.Now()
	// Reserving two buckets full of tokens should take exactly one second.
	for i := 0; i < 16; i++ {
		// 1 byte -> 8 bits, which is smaller than our token size. Should
		// be considered to be a single token.
		require.NoError(l.ReserveEgress(1))
	}
	require.InDelta(time.Second, time.Since(start), float64(50*time.Millisecond))
}

func TestLimiterReserveEgressErrorWhenBytesLargerThanBucket(t *testing.T) {
	require := require.New(t)

	config := Config{
		EgressBitsPerSec: 80, // 10 bytes.
		TokenSize:        10, // Bucket has 8 tokens.
	}
	l := NewLimiter(config)

	require.Error(l.ReserveEgress(12))
}
