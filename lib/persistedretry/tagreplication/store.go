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
package tagreplication

import (
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"

	"github.com/uber/kraken/lib/persistedretry"
)

// Store stores tags to be replicated asynchronously.
type Store struct {
	db *sqlx.DB
}

// NewStore creates a new Store.
func NewStore(db *sqlx.DB, rv RemoteValidator) (*Store, error) {
	s := &Store{db}
	if err := s.deleteInvalidTasks(rv); err != nil {
		return nil, fmt.Errorf("delete invalid tasks: %s", err)
	}
	return s, nil
}

// GetPending returns all pending tasks.
func (s *Store) GetPending() ([]persistedretry.Task, error) {
	return s.selectStatus("pending")
}

// GetFailed returns all failed tasks.
func (s *Store) GetFailed() ([]persistedretry.Task, error) {
	return s.selectStatus("failed")
}

// AddPending adds r as pending.
func (s *Store) AddPending(r persistedretry.Task) error {
	return s.addWithStatus(r, "pending")
}

// AddFailed adds r as failed.
func (s *Store) AddFailed(r persistedretry.Task) error {
	return s.addWithStatus(r, "failed")
}

// MarkPending marks r as pending.
func (s *Store) MarkPending(r persistedretry.Task) error {
	res, err := s.db.NamedExec(`
		UPDATE replicate_tag_task
		SET status = "pending"
		WHERE tag=:tag AND destination=:destination
	`, r.(*Task))
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		panic("driver does not support RowsAffected")
	} else if n == 0 {
		return persistedretry.ErrTaskNotFound
	}
	return nil
}

// MarkFailed marks r as failed.
func (s *Store) MarkFailed(r persistedretry.Task) error {
	t := r.(*Task)
	res, err := s.db.NamedExec(`
		UPDATE replicate_tag_task
		SET last_attempt = CURRENT_TIMESTAMP,
			failures = failures + 1,
			status = "failed"
		WHERE tag=:tag AND destination=:destination
	`, t)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		panic("driver does not support RowsAffected")
	} else if n == 0 {
		return persistedretry.ErrTaskNotFound
	}
	t.Failures++
	t.LastAttempt = time.Now()
	return nil
}

// Remove removes r.
func (s *Store) Remove(r persistedretry.Task) error {
	return s.delete(r)
}

// Find is not supported.
func (s *Store) Find(query interface{}) ([]persistedretry.Task, error) {
	return nil, errors.New("not supported")
}

func (s *Store) addWithStatus(r persistedretry.Task, status string) error {
	query := fmt.Sprintf(`
		INSERT INTO replicate_tag_task (
			tag,
			digest,
			dependencies,
			destination,
			last_attempt,
			failures,
			delay,
			status
		) VALUES (
			:tag,
			:digest,
			:dependencies,
			:destination,
			:last_attempt,
			:failures,
			:delay,
			%q
		)
	`, status)
	_, err := s.db.NamedExec(query, r.(*Task))
	if se, ok := err.(sqlite3.Error); ok {
		if se.ExtendedCode == sqlite3.ErrConstraintPrimaryKey {
			return persistedretry.ErrTaskExists
		}
	}
	return err
}

func (s *Store) selectStatus(status string) ([]persistedretry.Task, error) {
	var tasks []*Task
	err := s.db.Select(&tasks, `
		SELECT tag, digest, dependencies, destination, created_at, last_attempt, failures, delay
		FROM replicate_tag_task
		WHERE status=?`, status)
	if err != nil {
		return nil, err
	}
	var result []persistedretry.Task
	for _, t := range tasks {
		result = append(result, t)
	}
	return result, nil
}

// deleteInvalidTasks deletes replication tasks whose destinations are no longer
// valid remotes.
func (s *Store) deleteInvalidTasks(rv RemoteValidator) error {
	tasks := []*Task{}
	if err := s.db.Select(&tasks, `SELECT tag, destination FROM replicate_tag_task`); err != nil {
		return fmt.Errorf("select all tasks: %s", err)
	}
	for _, t := range tasks {
		if rv.Valid(t.Tag, t.Destination) {
			continue
		}
		if err := s.delete(t); err != nil {
			return fmt.Errorf("delete: %s", err)
		}
	}
	return nil
}

func (s *Store) delete(r persistedretry.Task) error {
	_, err := s.db.NamedExec(`
		DELETE FROM replicate_tag_task
		WHERE tag=:tag AND destination=:destination`, r.(*Task))
	return err
}
