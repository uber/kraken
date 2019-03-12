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

import "time"

// Task represents a single unit of work which must eventually succeed.
type Task interface {
	GetLastAttempt() time.Time
	GetFailures() int
	Ready() bool

	// Tags returns tags describing the context of the task, which can be
	// included on metrics to group related instances of a task.
	Tags() map[string]string
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

	// Find returns tasks which match a query.
	Find(query interface{}) ([]Task, error)
}

// Executor executes tasks.
type Executor interface {
	Exec(Task) error
	Name() string
}
