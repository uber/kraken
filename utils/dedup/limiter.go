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
package dedup

import (
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
)

// TaskGCInterval is the interval in which garbage collection of old tasks runs.
const TaskGCInterval = time.Minute

// TaskRunner runs against some input and produces some output w/ a ttl.
type TaskRunner interface {
	Run(input interface{}) (output interface{}, ttl time.Duration)
}

type task struct {
	input interface{}

	cond      *sync.Cond
	running   bool
	output    interface{}
	expiresAt time.Time
}

func newTask(input interface{}) *task {
	return &task{
		input: input,
		cond:  sync.NewCond(new(sync.Mutex)),
	}
}

func (t *task) expired(now time.Time) bool {
	return now.After(t.expiresAt)
}

// Limiter deduplicates the running of a common task within a given limit. Tasks
// are deduplicated based on input equality.
type Limiter struct {
	sync.RWMutex
	clk    clock.Clock
	runner TaskRunner
	tasks  map[interface{}]*task
	gc     *IntervalTrap
}

// NewLimiter creates a new Limiter for tasks. The limit is determined per task
// via the TaskRunner.
func NewLimiter(clk clock.Clock, runner TaskRunner) *Limiter {
	l := &Limiter{
		clk:    clk,
		runner: runner,
		tasks:  make(map[interface{}]*task),
	}
	l.gc = NewIntervalTrap(TaskGCInterval, clk, &limiterTaskGC{l})
	return l
}

// Run runs a task with input.
func (l *Limiter) Run(input interface{}) interface{} {
	l.gc.Trap()

	l.RLock()
	t, ok := l.tasks[input]
	l.RUnlock()
	if !ok {
		// Slow path, must initialize task struct under global write lock.
		l.Lock()
		t, ok = l.tasks[input]
		if !ok {
			t = newTask(input)
			l.tasks[input] = t
		}
		l.Unlock()
	}
	return l.getOutput(t)
}

func (l *Limiter) getOutput(t *task) interface{} {
	t.cond.L.Lock()

	if !t.expired(l.clk.Now()) {
		defer t.cond.L.Unlock()
		return t.output
	}

	if t.running {
		t.cond.Wait()
		defer t.cond.L.Unlock()
		return t.output
	}

	t.running = true
	t.cond.L.Unlock()

	output, ttl := l.runner.Run(t.input)

	t.cond.L.Lock()
	t.output = output
	t.expiresAt = l.clk.Now().Add(ttl)
	t.running = false
	t.cond.L.Unlock()

	t.cond.Broadcast()

	return output
}

type limiterTaskGC struct {
	limiter *Limiter
}

func (gc *limiterTaskGC) Run() {
	gc.limiter.Lock()
	defer gc.limiter.Unlock()

	for input, t := range gc.limiter.tasks {
		t.cond.L.Lock()
		expired := t.expired(gc.limiter.clk.Now()) && !t.running
		t.cond.L.Unlock()
		if expired {
			delete(gc.limiter.tasks, input)
		}
	}
}
