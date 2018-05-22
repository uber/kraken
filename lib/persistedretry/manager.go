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

type queue struct {
	tasks   chan Task
	counter tally.Counter
}

func newQueue(size int, counter tally.Counter) *queue {
	return &queue{make(chan Task, size), counter}
}

type manager struct {
	config   Config
	stats    tally.Scope
	store    Store
	executor Executor

	wg sync.WaitGroup

	incoming *queue
	retries  *queue

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
		incoming: newQueue(config.IncomingBuffer, stats.Counter("incoming")),
		retries:  newQueue(config.RetryBuffer, stats.Counter("retries")),
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
	if !t.Ready() {
		if err := m.store.MarkFailed(t); err != nil {
			return fmt.Errorf("mark unready task as failed: %s", err)
		}
		return nil
	}
	if err := m.enqueue(t, m.incoming); err != nil {
		return fmt.Errorf("enqueue: %s", err)
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

func (m *manager) enqueue(t Task, q *queue) error {
	if err := m.store.MarkPending(t); err != nil {
		return fmt.Errorf("mark task as pending: %s", err)
	}
	select {
	case q.tasks <- t:
		q.counter.Inc(1)
	default:
		// If task queue is full, fallback task to failure state so it can be
		// picked up by a retry round.
		if err := m.store.MarkFailed(t); err != nil {
			return fmt.Errorf("mark task as failed: %s", err)
		}
	}
	return nil
}

func (m *manager) worker(q *queue, limit time.Duration) {
	defer m.wg.Done()

	for {
		select {
		case <-m.done:
			return
		case t := <-q.tasks:
			q.counter.Inc(-1)
			if err := m.exec(t); err != nil {
				m.stats.Counter("task_failure").Inc(1)
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
			if err := m.enqueue(t, m.retries); err != nil {
				log.With("task", t).Errorf("Error adding retry task: %s", err)
			}
		}
	}
}

func (m *manager) exec(t Task) error {
	timer := m.stats.Timer("exec").Start()
	defer timer.Stop()

	if err := m.executor.Exec(t); err != nil {
		if err := m.store.MarkFailed(t); err != nil {
			return fmt.Errorf("mark task as failed: %s", err)
		}
		log.With("task", t).Errorf("Task failed: %s", err)
		return nil
	}

	if err := m.store.MarkDone(t); err != nil {
		return fmt.Errorf("mark task as done: %s", err)
	}
	return nil
}
