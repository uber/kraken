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
)

func TestIntervalTrap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	interval := time.Minute
	clk := clock.NewMock()
	clk.Set(time.Now())
	task := mockdedup.NewMockIntervalTask(ctrl)

	trap := NewIntervalTrap(interval, clk, task)

	trap.Trap() // Noop.
	trap.Trap() // Noop.

	clk.Add(interval + 1)
	task.EXPECT().Run()
	trap.Trap()
	trap.Trap() // Noop.

	clk.Add(interval / 2)
	trap.Trap() // Noop.

	clk.Add(interval + 1)
	task.EXPECT().Run()
	trap.Trap()
	trap.Trap() // Noop.
}

func TestIntervalTrapConcurrency(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	task := mockdedup.NewMockIntervalTask(ctrl)

	trap := NewIntervalTrap(100*time.Millisecond, clock.New(), task)

	task.EXPECT().Run().Times(4)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 4; k++ {
				// Guarantees that Trap() will be called exactly 4 times,
				// as the interval between each Trap() call is >= 100ms
				// for each goroutine and the total interval for a given
				// goroutine will never reach 500ms.
				time.Sleep(120*time.Millisecond +
					randutil.Duration(10*time.Millisecond))
				trap.Trap()
			}
		}()
	}
	wg.Wait()
}
