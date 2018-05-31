package writeback

import (
	"fmt"
	"time"

	"code.uber.internal/infra/kraken/core"
)

// Task contains information to write back a blob to remote storage.
type Task struct {
	Namespace   string        `db:"namespace"`
	Digest      core.Digest   `db:"digest"`
	CreatedAt   time.Time     `db:"created_at"`
	LastAttempt time.Time     `db:"last_attempt"`
	Failures    int           `db:"failures"`
	Delay       time.Duration `db:"delay"`
}

// NewTask creates a new Task.
func NewTask(namespace string, d core.Digest) *Task {
	return NewTaskWithDelay(namespace, d, 0)
}

// NewTaskWithDelay creates a new Task which will run after a given delay.
func NewTaskWithDelay(namespace string, d core.Digest, delay time.Duration) *Task {
	return &Task{
		Namespace: namespace,
		Digest:    d,
		CreatedAt: time.Now(),
		Delay:     delay,
	}
}

func (t *Task) String() string {
	return fmt.Sprintf("writeback.Task(namespace=%s, digest=%s)", t.Namespace, t.Digest)
}

// GetLastAttempt returns when t was last attempted.
func (t *Task) GetLastAttempt() time.Time {
	return t.LastAttempt
}

// Ready returns whether t is ready to run.
func (t *Task) Ready() bool {
	return time.Since(t.CreatedAt) >= t.Delay
}
