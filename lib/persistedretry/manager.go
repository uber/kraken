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

// Config defines Manager configuration.
type Config struct {
	NumWorkers        int           `yaml:"num_workers"`
	NumRetryWorkers   int           `yaml:"num_retry_workers"`
	TaskChanSize      int           `yaml:"task_chan_size"`
	RetryChanSize     int           `yaml:"retry_chan_size"`
	TaskInterval      time.Duration `yaml:"task_interval"`
	RetryInterval     time.Duration `yaml:"retry_interval"`
	RetryTaskInterval time.Duration `yaml:"retry_task_interval"`
}

func (c Config) applyDefaults() Config {
	if c.NumWorkers == 0 {
		c.NumWorkers = 6
	}
	if c.NumRetryWorkers == 0 {
		c.NumRetryWorkers = 1
	}
	if c.TaskInterval == 0 {
		c.TaskInterval = 10 * time.Millisecond
	}
	if c.RetryInterval == 0 {
		c.RetryInterval = 5 * time.Minute
	}
	if c.RetryTaskInterval == 0 {
		c.RetryTaskInterval = 5 * time.Second
	}
	return c
}

// Manager defines interface for a persisted retry manager.
type Manager interface {
	Add(Task) error
	Close()
}

type manager struct {
	config Config
	stats  tally.Scope
	store  Store

	wg      sync.WaitGroup
	tasks   chan Task
	retries chan Task

	closeOnce sync.Once
	done      chan struct{}
	closed    atomic.Bool
}

// New creates a new pool with default num workers and chan size.
func New(config Config, stats tally.Scope, store Store) (Manager, error) {
	config = config.applyDefaults()
	m := &manager{
		config:  config,
		stats:   stats,
		store:   store,
		tasks:   make(chan Task, config.TaskChanSize),
		retries: make(chan Task, config.RetryChanSize),
		done:    make(chan struct{}),
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
			if err := m.run(t); err != nil {
				m.stats.Counter("task_failure").Inc(1)
				log.With("task", t).Errorf("Failed to run task: %s", err)
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
			if err := m.run(t); err != nil {
				m.stats.Counter("retry_failure").Inc(1)
				log.With("task", t).Error("Failed to retry task: %s", err)
			}
			time.Sleep(m.config.RetryTaskInterval)
		}
	}
}

func (m *manager) run(task Task) error {
	if err := task.Run(); err != nil {
		if err := m.store.MarkFailed(task); err != nil {
			return fmt.Errorf("mark task as failed: %s", err)
		}
		log.With("task", task).Errorf("Fask failed: %s", err)
		return nil
	}

	if err := m.store.MarkDone(task); err != nil {
		return fmt.Errorf("mark task as done: %s", err)
	}
	return nil
}
