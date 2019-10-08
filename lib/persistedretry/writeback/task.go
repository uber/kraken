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
package writeback

import (
	"fmt"
	"time"

	"github.com/uber/kraken/core"
)

// Task contains information to write back a blob to remote storage.
type Task struct {
	Namespace   string        `db:"namespace"`
	Name        string        `db:"name"`
	CreatedAt   time.Time     `db:"created_at"`
	LastAttempt time.Time     `db:"last_attempt"`
	Failures    int           `db:"failures"`
	Delay       time.Duration `db:"delay"`

	// Deprecated. Use name instead.
	Digest core.Digest `db:"digest"`
}

// NewTask creates a new Task.
func NewTask(namespace, name string, delay time.Duration) *Task {
	return &Task{
		Namespace: namespace,
		Name:      name,
		CreatedAt: time.Now(),
		Delay:     delay,
	}
}

func (t *Task) String() string {
	return fmt.Sprintf("writeback.Task(namespace=%s, name=%s)", t.Namespace, t.Name)
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

// Tags is unused.
func (t *Task) Tags() map[string]string {
	return nil
}
