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
package persistedretry

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"

	"github.com/uber/kraken/utils/log"
)

// ErrManagerClosed is returned when Add is called on a closed manager.
var ErrManagerClosed = errors.New("manager closed")

// Manager defines interface for a persisted retry manager.
type Manager interface {
	Add(Task) error
	SyncExec(Task) error
	Close()
	Find(query interface{}) ([]Task, error)
}

type manager struct {
	config   Config
	stats    tally.Scope
	store    Store
	executor Executor

	wg sync.WaitGroup

	incoming chan Task
	retries  chan Task

	closeOnce sync.Once
	done      chan struct{}
	closed    atomic.Bool
}

// NewManager creates a new Manager.
func NewManager(
	config Config, stats tally.Scope, store Store, executor Executor) (Manager, error) {

	stats = stats.Tagged(map[string]string{
		"module":   "persistedretry",
		"executor": executor.Name(),
	})
	config = config.applyDefaults()
	m := &manager{
		config:   config,
		stats:    stats,
		store:    store,
		executor: executor,
		incoming: make(chan Task, config.IncomingBuffer),
		retries:  make(chan Task, config.RetryBuffer),
		done:     make(chan struct{}),
	}
	if err := m.markPendingTasksAsFailed(); err != nil {
		return nil, fmt.Errorf("mark pending tasks as failed: %s", err)
	}
	if err := m.start(); err != nil {
		return nil, fmt.Errorf("start: %s", err)
	}
	return m, nil
}

func (m *manager) markPendingTasksAsFailed() error {
	tasks, err := m.store.GetPending()
	if err != nil {
		return fmt.Errorf("get pending tasks: %s", err)
	}
	for _, t := range tasks {
		if err := m.store.MarkFailed(t); err != nil {
			return fmt.Errorf("mark task as failed: %s", err)
		}
	}
	return nil
}

// start starts workers and retry.
func (m *manager) start() error {
	if m.closed.Load() {
		return ErrManagerClosed
	}

	totalWorkers := m.config.NumIncomingWorkers + m.config.NumRetryWorkers
	limit := m.config.MaxTaskThroughput * time.Duration(totalWorkers)

	for i := 0; i < m.config.NumIncomingWorkers; i++ {
		m.wg.Add(1)
		go m.worker(m.incoming, limit)
	}
	for i := 0; i < m.config.NumRetryWorkers; i++ {
		m.wg.Add(1)
		go m.worker(m.retries, limit)
	}

	m.wg.Add(1)
	go m.tickerLoop()

	return nil
}

// Add enqueues an incoming task to be executed.
func (m *manager) Add(t Task) error {
	if m.closed.Load() {
		return ErrManagerClosed
	}
	ready := t.Ready()
	var err error
	if ready {
		err = m.store.AddPending(t)
	} else {
		err = m.store.AddFailed(t)
	}
	if err != nil {
		if err == ErrTaskExists {
			// No-op on duplicate tasks.
			return nil
		}
		return fmt.Errorf("store: %s", err)
	}
	if ready {
		if err := m.enqueue(t, m.incoming); err != nil {
			return fmt.Errorf("enqueue: %s", err)
		}
	}
	return nil
}

// SyncExec executes the task synchronously.
// Tasks will NOT be added to the retry queue if fail.
func (m *manager) SyncExec(t Task) error {
	return m.executor.Exec(t)
}

// Close waits for all workers to exit current task.
func (m *manager) Close() {
	m.closeOnce.Do(func() {
		m.closed.Store(true)
		close(m.done)
		m.wg.Wait()
	})
}

func (m *manager) Find(query interface{}) ([]Task, error) {
	return m.store.Find(query)
}

func (m *manager) enqueue(t Task, tasks chan Task) error {
	select {
	case tasks <- t:
	default:
		// If task queue is full, fallback task to failure state so it can be
		// picked up by a retry round.
		if err := m.store.MarkFailed(t); err != nil {
			return fmt.Errorf("mark task as failed: %s", err)
		}
	}
	return nil
}

func (m *manager) retry(t Task) error {
	if err := m.store.MarkPending(t); err != nil {
		return fmt.Errorf("mark pending: %s", err)
	}
	if err := m.enqueue(t, m.retries); err != nil {
		return fmt.Errorf("enqueue: %s", err)
	}
	return nil
}

func (m *manager) worker(tasks chan Task, limit time.Duration) {
	defer m.wg.Done()

	for {
		select {
		case <-m.done:
			return
		case t := <-tasks:
			if err := m.exec(t); err != nil {
				m.stats.Counter("exec_failures").Inc(1)
				log.With("task", t).Errorf("Failed to exec task: %s", err)
			}
			time.Sleep(limit)
		}
	}
}

func (m *manager) tickerLoop() {
	defer m.wg.Done()

	pollRetriesTicker := time.NewTicker(m.config.PollRetriesInterval)
	for {
		select {
		case <-m.done:
			return
		case <-pollRetriesTicker.C:
			m.pollRetries()
		}
	}
}

func (m *manager) pollRetries() {
	tasks, err := m.store.GetFailed()
	if err != nil {
		m.stats.Counter("get_failed_failure").Inc(1)
		log.Errorf("Error getting failed tasks: %s", err)
		return
	}
	for _, t := range tasks {
		if t.Ready() && time.Since(t.GetLastAttempt()) > m.config.RetryInterval {
			if err := m.retry(t); err != nil {
				log.With("task", t).Errorf("Error adding retry task: %s", err)
			}
		}
	}
}

func (m *manager) exec(t Task) error {
	if err := m.executor.Exec(t); err != nil {
		if err := m.store.MarkFailed(t); err != nil {
			return fmt.Errorf("mark task as failed: %s", err)
		}
		log.With(
			"task", t,
			"failures", t.GetFailures()).Errorf("Task failed: %s", err)
		m.stats.Tagged(t.Tags()).Counter("task_failures").Inc(1)
		return nil
	}
	if err := m.store.Remove(t); err != nil {
		return fmt.Errorf("remove task: %s", err)
	}
	return nil
}
