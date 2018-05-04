package dedup

import (
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
)

// IntervalTask defines a task to run on some interval.
type IntervalTask interface {
	Run()
}

// IntervalTrap manages trapping into some task which must run at a given interval.
type IntervalTrap struct {
	sync.RWMutex
	clk      clock.Clock
	interval time.Duration
	prev     time.Time
	task     IntervalTask
}

// NewIntervalTrap creates a new IntervalTrap.
func NewIntervalTrap(
	interval time.Duration, clk clock.Clock, task IntervalTask) *IntervalTrap {

	return &IntervalTrap{
		clk:      clk,
		interval: interval,
		prev:     clk.Now(),
		task:     task,
	}
}

func (t *IntervalTrap) ready() bool {
	return t.clk.Now().After(t.prev.Add(t.interval))
}

// Trap quickly checks if the interval has passed since the last task run, and if
// it has, it runs the task.
func (t *IntervalTrap) Trap() {
	t.RLock()
	ready := t.ready()
	t.RUnlock()
	if !ready {
		return
	}

	t.Lock()
	defer t.Unlock()
	if !t.ready() {
		return
	}
	t.task.Run()
	t.prev = t.clk.Now()
}
