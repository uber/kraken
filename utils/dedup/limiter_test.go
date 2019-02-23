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
package dedup_test

import (
	"sync"
	"testing"
	"time"

	"github.com/uber/kraken/mocks/utils/dedup"
	. "github.com/uber/kraken/utils/dedup"
	"github.com/uber/kraken/utils/randutil"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestLimiter(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runner := mockdedup.NewMockTaskRunner(ctrl)

	limiter := NewLimiter(clock.New(), runner)

	input := "some input"
	output := "some output"

	// TODO: We discovered that changing Times(4) to AnyTimes() then this test won't hang and panic
	// but will instead fail with message:
	// Error Trace:	limiter_test.go:44
	// Error:      	Max difference between 2s and 2.277642953s allowed is 2.5e+08, but difference was -2.77642953e+08

	// TODO: Changing the amount of times the loop runs to 100 (instead of 1000) prevents the test from hanging
	// but still, something is probably wrong with some part of this test.
	runner.EXPECT().Run(input).Return(output, 500*time.Millisecond).Times(4)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
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
	ttl  time.Duration
}

func (r *testRunner) Run(input interface{}) (interface{}, time.Duration) {
	<-r.stop
	return input, r.ttl
}

func TestLimiterLongRunningTask(t *testing.T) {
	require := require.New(t)

	runner := &testRunner{make(chan bool), time.Second}

	limiter := NewLimiter(clock.New(), runner)

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

	limiter := NewLimiter(clk, runner)

	input := "some input"
	output := "some output"
	ttl := 100 * time.Millisecond

	runner.EXPECT().Run(input).Return(output, ttl)
	require.Equal(output, limiter.Run(input))
	require.Equal(output, limiter.Run(input))

	clk.Add(TaskGCInterval + 1)
	runner.EXPECT().Run(input).Return(output, ttl)
	require.Equal(output, limiter.Run(input))
	require.Equal(output, limiter.Run(input))
}

func max(a, b time.Duration) time.Duration {
	if a < b {
		return b
	}
	return a
}
