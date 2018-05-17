package tagreplicate

import (
	"fmt"
	"time"

	"code.uber.internal/infra/kraken/core"
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
}

// NewTask creates a new Task.
func NewTask(
	tag string,
	d core.Digest,
	dependencies core.DigestList,
	destination string) *Task {

	return &Task{
		Tag:          tag,
		Digest:       d,
		Dependencies: dependencies,
		Destination:  destination,
		CreatedAt:    time.Now(),
		LastAttempt:  time.Now(),
	}
}

func (t *Task) String() string {
	return fmt.Sprintf("tagreplicate.Task(tag=%s, dest=%s)", t.Tag, t.Destination)
}
