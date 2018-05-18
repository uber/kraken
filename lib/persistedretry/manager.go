package persistedretry

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"

	"code.uber.internal/infra/kraken/utils/log"
)

// ErrManagerClosed is returned when Add is called on a closed manager.
var ErrManagerClosed = errors.New("manager closed")

// Manager defines interface for a persisted retry manager.
type Manager interface {
	Add(Task) error
	Close()
}

type manager struct {
	config   Config
	stats    tally.Scope
	store    Store
	executor Executor

	wg      sync.WaitGroup
	tasks   chan Task
	retries chan Task

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
		tasks:    make(chan Task, config.TaskChanSize),
		retries:  make(chan Task, config.RetryChanSize),
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

	for i := 0; i < m.config.NumWorkers; i++ {
		m.wg.Add(1)
		go m.startWorker()
	}

	for i := 0; i < m.config.NumRetryWorkers; i++ {
		m.wg.Add(1)
		go m.startRetryWorker()
	}

	m.wg.Add(1)
	go m.populateRetries()

	return nil
}

// Add adds a task in the pool for works to pick up. It is non-blocking.
func (m *manager) Add(task Task) error {
	if m.closed.Load() {
		return ErrManagerClosed
	}

	if err := m.store.MarkPending(task); err != nil {
		return fmt.Errorf("mark task as pending: %s", err)
	}

	select {
	case m.tasks <- task:
		m.stats.Counter("tasks").Inc(1)
	default:
		// If task queue is full, fallback task to failure state so
		// it can be picked up by a retry round.
		if err := m.store.MarkFailed(task); err != nil {
			return fmt.Errorf("mark task as failed: %s", err)
		}
	}
	return nil
}

// Close waits for all workers to exit current task.
func (m *manager) Close() {
	m.closeOnce.Do(func() {
		m.closed.Store(true)
		close(m.done)
		m.wg.Wait()
	})
}

func (m *manager) startWorker() {
	defer m.wg.Done()

	for {
		select {
		case <-m.done:
			return
		case t := <-m.tasks:
			m.stats.Counter("tasks").Inc(-1)
			if err := m.exec(t); err != nil {
				m.stats.Counter("task_failure").Inc(1)
				log.With("task", t).Errorf("Failed to exec task: %s", err)
			}
			time.Sleep(m.config.TaskInterval)
		}
	}
}

func (m *manager) populateRetries() {
	defer m.wg.Done()

	retryTicker := time.NewTicker(m.config.RetryInterval)
	for {
		select {
		case <-m.done:
			return
		case <-retryTicker.C:
			tasks, err := m.store.GetFailed()
			if err != nil {
				m.stats.Counter("get_failed_failure").Inc(1)
				log.Errorf("Error getting failed tasks: %s", err)
				continue
			}
			m.addRetries(tasks)
		}
	}
}

// addRetries adds a list of tasks to retries channel. It is non-blocking.
func (m *manager) addRetries(tasks []Task) {
	for _, t := range tasks {
		// Mark tasks as pending before the task enter the queue to avoid duplicated retry.
		if err := m.store.MarkPending(t); err != nil {
			m.stats.Counter("mark_pending_failure").Inc(1)
			log.With("task", t).Errorf("Failed to mark task as pending: %s", err)
			continue
		}
		select {
		case m.retries <- t:
			m.stats.Counter("retries").Inc(1)
		default:
			// If retry queue is full, fallback task to failure state so
			// it can be picked up by next retry round.
			if err := m.store.MarkFailed(t); err != nil {
				m.stats.Counter("mark_failed_failure").Inc(1)
				log.With("task", t).Errorf("Failed to mark task as failed: %s", err)
			}
			return
		}
	}
}

// TODO: add backoff base on failure count or last attempt.
func (m *manager) startRetryWorker() {
	defer m.wg.Done()

	for {
		select {
		case <-m.done:
			return
		case t := <-m.retries:
			m.stats.Counter("retries").Inc(-1)
			if err := m.exec(t); err != nil {
				m.stats.Counter("retry_failure").Inc(1)
				log.With("task", t).Error("Failed to retry task: %s", err)
			}
			time.Sleep(m.config.RetryTaskInterval)
		}
	}
}

func (m *manager) exec(task Task) error {
	timer := m.stats.Timer("exec").Start()
	defer timer.Stop()

	if err := m.executor.Exec(task); err != nil {
		if err := m.store.MarkFailed(task); err != nil {
			return fmt.Errorf("mark task as failed: %s", err)
		}
		log.With("task", task).Errorf("Task failed: %s", err)
		return nil
	}

	if err := m.store.MarkDone(task); err != nil {
		return fmt.Errorf("mark task as done: %s", err)
	}
	return nil
}
