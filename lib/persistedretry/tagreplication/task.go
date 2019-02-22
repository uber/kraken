package tagreplication

import (
	"fmt"
	"time"

	"github.com/uber/kraken/core"
)

// Task contains information to replicate a tag and its dependencies to a
// remote destination.
type Task struct {
	Tag          string          `db:"tag"`
	Digest       core.Digest     `db:"digest"`
	Dependencies core.DigestList `db:"dependencies"`
	Destination  string          `db:"destination"`
	CreatedAt    time.Time       `db:"created_at"`
	LastAttempt  time.Time       `db:"last_attempt"`
	Failures     int             `db:"failures"`
	Delay        time.Duration   `db:"delay"`
}

// NewTask creates a new Task.
func NewTask(
	tag string,
	d core.Digest,
	dependencies core.DigestList,
	destination string,
	delay time.Duration) *Task {

	return &Task{
		Tag:          tag,
		Digest:       d,
		Dependencies: dependencies,
		Destination:  destination,
		CreatedAt:    time.Now(),
		Delay:        delay,
	}
}

func (t *Task) String() string {
	return fmt.Sprintf("tagreplication.Task(tag=%s, dest=%s)", t.Tag, t.Destination)
}

// GetLastAttempt returns when t was last attempted.
func (t *Task) GetLastAttempt() time.Time {
	return t.LastAttempt
}

// GetFailures returns the number of times t has failed.
func (t *Task) GetFailures() int {
	return t.Failures
}

// Ready returns whether t is ready to run.
func (t *Task) Ready() bool {
	return time.Since(t.CreatedAt) >= t.Delay
}
