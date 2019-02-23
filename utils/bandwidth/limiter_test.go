// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

func TestLimiterInvalidConfig(t *testing.T) {
	require := require.New(t)

	bps := uint64(800) // 100 bytes.

	_, err := NewLimiter(Config{
		EgressBitsPerSec:  0,
		IngressBitsPerSec: bps,
		TokenSize:         1,
		Enable:            true,
	})
	require.Error(err)

	_, err = NewLimiter(Config{
		EgressBitsPerSec:  bps,
		IngressBitsPerSec: 0,
		TokenSize:         1,
		Enable:            true,
	})
	require.Error(err)
}

func TestLimiterDisabled(t *testing.T) {
	require := require.New(t)

	bps := uint64(800) // 100 bytes.

	l, err := NewLimiter(Config{
		EgressBitsPerSec:  bps,
		IngressBitsPerSec: bps,
		TokenSize:         1,
		Enable:            false,
	})
	require.NoError(err)
	require.Nil(l.egress)
	require.Nil(l.ingress)
	require.NoError(reserve(l, 1, egress))
	require.NoError(reserve(l, 1, ingress))
}

func TestLimiterReserveConcurrency(t *testing.T) {
	t.Parallel()

	for _, direction := range []string{egress, ingress} {
		t.Run(direction, func(t *testing.T) {
			require := require.New(t)

			bps := uint64(800) // 100 bytes.

			l, err := NewLimiter(Config{
				EgressBitsPerSec:  bps,
				IngressBitsPerSec: bps,
				TokenSize:         1,
				Enable:            true,
			})
			require.NoError(err)

			// This test starts a bunch of goroutines and see how many bytes
			// they can reserve in nsecs.
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

			l, err := NewLimiter(Config{
				EgressBitsPerSec:  bps,
				IngressBitsPerSec: bps,
				TokenSize:         10, // Bucket has 8 tokens.
				Enable:            true,
			})
			require.NoError(err)

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

			l, err := NewLimiter(Config{
				EgressBitsPerSec:  bps,
				IngressBitsPerSec: bps,
				TokenSize:         10, // Bucket has 8 tokens.
				Enable:            true,
			})
			require.NoError(err)

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

			l, err := NewLimiter(Config{
				EgressBitsPerSec:  bps,
				IngressBitsPerSec: bps,
				TokenSize:         10, // Bucket has 8 tokens.
				Enable:            true,
			})
			require.NoError(err)

			require.Error(reserve(l, 12, direction))
		})
	}
}

func TestLimiterAdjustError(t *testing.T) {
	require := require.New(t)

	l, err := NewLimiter(Config{
		EgressBitsPerSec:  50,
		IngressBitsPerSec: 10,
		TokenSize:         1,
		Enable:            true,
	})
	require.NoError(err)
	require.Error(l.Adjust(0))
}

func TestLimiterAdjust(t *testing.T) {
	require := require.New(t)

	l, err := NewLimiter(Config{
		EgressBitsPerSec:  50,
		IngressBitsPerSec: 10,
		TokenSize:         1,
		Enable:            true,
	})
	require.NoError(err)

	// No subtests since we want to ensure the calls don't affect each other.
	cases := []struct {
		denom   int
		egress  int64
		ingress int64
	}{
		{10, 5, 1},
		{5, 10, 2},
		{100, 1, 1},
	}
	for _, c := range cases {
		require.NoError(l.Adjust(c.denom))
		require.Equal(c.egress, l.EgressLimit())
		require.Equal(c.ingress, l.IngressLimit())
	}
}
