package dedup_test

import (
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/mocks/utils/dedup"
	. "code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/randutil"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestLimiter(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runner := mockdedup.NewMockTaskRunner(ctrl)

	limiter := NewLimiter(500*time.Millisecond, clock.New(), runner)

	input := "some input"
	output := "some output"

	runner.EXPECT().Run(input).Return(output).Times(4)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(randutil.Duration(max(1, 2*time.Second-time.Since(start))))
			require.Equal(output, limiter.Run(input))
		}()
	}
	wg.Wait()

	require.InDelta(2*time.Second, time.Since(start), float64(250*time.Millisecond))
}

type testRunner struct {
	stop chan bool
}

func (r *testRunner) Run(input interface{}) interface{} {
	<-r.stop
	return input
}

func TestLimiterLongRunningTask(t *testing.T) {
	require := require.New(t)

	runner := &testRunner{make(chan bool)}

	limiter := NewLimiter(time.Second, clock.New(), runner)

	input := "some input"

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.Equal(input, limiter.Run(input))
		}()
	}
	time.Sleep(time.Second)

	// All threads should wait on condition variable and, once the task stops,
	// immediately access the output and exit.
	start := time.Now()
	runner.stop <- true
	wg.Wait()
	require.True(time.Since(start) < 100*time.Millisecond)
}

func TestLimiterTaskGC(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clk := clock.NewMock()
	runner := mockdedup.NewMockTaskRunner(ctrl)

	limiter := NewLimiter(100*time.Millisecond, clk, runner)

	input := "some input"
	output := "some output"

	runner.EXPECT().Run(input).Return(output)
	require.Equal(output, limiter.Run(input))
	require.Equal(output, limiter.Run(input))

	clk.Add(TaskGCInterval + 1)
	runner.EXPECT().Run(input).Return(output)
	require.Equal(output, limiter.Run(input))
	require.Equal(output, limiter.Run(input))
}

func max(a, b time.Duration) time.Duration {
	if a < b {
		return b
	}
	return a
}
