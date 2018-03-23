package bandwidth

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	egress  = "egress"
	ingress = "ingress"
)

func reserve(l *Limiter, nbytes int64, direction string) error {
	if direction == egress {
		return l.ReserveEgress(nbytes)
	}
	return l.ReserveIngress(nbytes)
}

func TestLimiterReserveConcurrency(t *testing.T) {
	t.Parallel()

	for _, direction := range []string{egress, ingress} {
		t.Run(direction, func(t *testing.T) {
			require := require.New(t)

			bps := uint64(800) // 100 bytes.

			l := NewLimiter(Config{
				EgressBitsPerSec:  bps,
				IngressBitsPerSec: bps,
				TokenSize:         1,
			})

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
						require.NoError(reserve(l, 1, direction))
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
			require.InDelta(bps*uint64(nsecs+1), 8*nbytes, 10.0)
		})
	}
}

func TestLimiterReserveBytesTokenScaling(t *testing.T) {
	t.Parallel()

	for _, direction := range []string{egress, ingress} {
		t.Run(direction, func(t *testing.T) {
			require := require.New(t)

			bps := uint64(80) // 10 bytes.

			l := NewLimiter(Config{
				EgressBitsPerSec:  bps,
				IngressBitsPerSec: bps,
				TokenSize:         10, // Bucket has 8 tokens.
			})

			start := time.Now()
			// Reserving two buckets full of tokens should take exactly one second.
			for i := 0; i < 4; i++ {
				// 6 bytes -> 48 bits, which is should be equal to 4 tokens.
				require.NoError(reserve(l, 6, direction))
			}
			require.InDelta(time.Second, time.Since(start), float64(50*time.Millisecond))
		})
	}
}

func TestLimiterReserveBytesSmallerThanTokenSize(t *testing.T) {
	t.Parallel()

	for _, direction := range []string{egress, ingress} {
		t.Run(direction, func(t *testing.T) {
			require := require.New(t)

			bps := uint64(80) // 10 bytes.

			l := NewLimiter(Config{
				EgressBitsPerSec:  bps,
				IngressBitsPerSec: bps,
				TokenSize:         10, // Bucket has 8 tokens.
			})

			start := time.Now()
			// Reserving two buckets full of tokens should take exactly one second.
			for i := 0; i < 16; i++ {
				// 1 byte -> 8 bits, which is smaller than our token size. Should
				// be considered to be a single token.
				require.NoError(reserve(l, 1, direction))
			}
			require.InDelta(time.Second, time.Since(start), float64(50*time.Millisecond))
		})
	}
}

func TestLimiterReserveErrorWhenBytesLargerThanBucket(t *testing.T) {
	t.Parallel()

	for _, direction := range []string{egress, ingress} {
		t.Run(direction, func(t *testing.T) {
			require := require.New(t)

			bps := uint64(80) // 10 bytes.

			l := NewLimiter(Config{
				EgressBitsPerSec:  bps,
				IngressBitsPerSec: bps,
				TokenSize:         10, // Bucket has 8 tokens.
			})

			require.Error(reserve(l, 12, direction))
		})
	}
}
