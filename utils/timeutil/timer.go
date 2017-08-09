package timeutil

import (
	"sync"
	"time"
)

// Timer is a thread-safe adaptation of time.Timer intended for timeouts which
// may be periodically invalidated. A Timer can be started and cancelled multiple
// times before the Timer fires. Once a Timer fires, it cannot be used again.
type Timer struct {
	// C will be closed once the Timer fires.
	C chan struct{}

	sync.Mutex
	timer    *time.Timer
	cancel   chan bool
	duration time.Duration
}

// NewTimer creates a new Timer which is set to the given duration.
func NewTimer(d time.Duration) *Timer {
	return &Timer{
		C:        make(chan struct{}),
		cancel:   make(chan bool),
		duration: d,
	}
}

// Start starts the Timer. Returns false if the timer has already started, or
// if the timer has already fired.
func (t *Timer) Start() bool {
	t.Lock()
	defer t.Unlock()

	if t.timer != nil {
		// Timer has already started.
		return false
	}
	t.timer = time.NewTimer(t.duration)

	// Must copy this reference since t.timer will be nil if Cancel is called.
	c := t.timer.C

	go func() {
		select {
		case <-c:
			close(t.C)
		case <-t.cancel:
		}
	}()

	return true
}

// Cancel cancels the Timer. Returns false if the timer has not started, or
// if the timer has already fired.
func (t *Timer) Cancel() bool {
	t.Lock()
	defer t.Unlock()

	if t.timer == nil {
		// Timer has not started.
		return false
	}
	if !t.timer.Stop() {
		// Timer already fired.
		return false
	}
	// Let the goroutine created by Start exit.
	t.cancel <- true
	t.timer = nil
	return true
}
