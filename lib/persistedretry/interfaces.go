package persistedretry

import "time"

// Task represents a single unit of work which must eventually succeed.
type Task interface {
	GetLastAttempt() time.Time
	Ready() bool
}

// Store provides persisted storage for tasks.
type Store interface {
	// AddPending adds a new task as pending in the store. Implementations should
	// return ErrTaskExists if the task is already in the store.
	AddPending(Task) error

	// AddFailed adds a new task as failed in the store. Implementations should
	// return ErrTaskExists if the task is already in the store.
	AddFailed(Task) error

	// MarkPending marks an existing task as pending.
	MarkPending(Task) error

	// MarkFailed marks an existing task as failed.
	MarkFailed(Task) error

	// GetPending returns all pending Tasks.
	GetPending() ([]Task, error)

	// GetFailed returns all failed Tasks.
	GetFailed() ([]Task, error)

	// Remove removes a task from the store.
	Remove(Task) error

	// Close closes the store.
	Close() error
}

// Executor executes tasks.
type Executor interface {
	Exec(Task) error
	Name() string
}
