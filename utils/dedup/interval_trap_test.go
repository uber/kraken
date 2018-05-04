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

	trap := NewIntervalTrap(200*time.Millisecond, clock.New(), task)

	task.EXPECT().Run().Times(4)

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(randutil.Duration(850 * time.Millisecond))
			trap.Trap()
		}()
	}
	wg.Wait()
}
