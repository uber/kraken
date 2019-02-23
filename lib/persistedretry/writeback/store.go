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
	"errors"
	"fmt"
	"time"

	"github.com/uber/kraken/lib/persistedretry"

	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
)

// Store stores writeback tasks.
type Store struct {
	db *sqlx.DB
}

// NewStore creates a new Store.
func NewStore(db *sqlx.DB) *Store {
	return &Store{db}
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
		UPDATE writeback_task
		SET status = "pending"
		WHERE namespace=:namespace AND name=:name
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
		UPDATE writeback_task
		SET last_attempt = CURRENT_TIMESTAMP,
			failures = failures + 1,
			status = "failed"
		WHERE namespace=:namespace AND name=:name
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
	_, err := s.db.NamedExec(`
		DELETE FROM writeback_task
		WHERE namespace=:namespace AND name=:name
	`, r.(*Task))
	return err
}

// Find finds tasks matching query.
func (s *Store) Find(query interface{}) ([]persistedretry.Task, error) {
	var tasks []*Task
	var err error
	switch q := query.(type) {
	case *NameQuery:
		err = s.db.Select(&tasks, `
			SELECT namespace, name, created_at, last_attempt, failures, delay
			FROM writeback_task
			WHERE name=?
		`, q.name)
	default:
		return nil, errors.New("unknown query type")
	}
	if err != nil {
		return nil, err
	}
	return convert(tasks), nil
}

func (s *Store) addWithStatus(r persistedretry.Task, status string) error {
	query := fmt.Sprintf(`
		INSERT INTO writeback_task (
			namespace,
			name,
			last_attempt,
			failures,
			delay,
			status
		) VALUES (
			:namespace,
			:name,
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
		SELECT namespace, name, created_at, last_attempt, failures, delay
		FROM writeback_task
		WHERE status=?
	`, status)
	if err != nil {
		return nil, err
	}
	return convert(tasks), nil
}

func convert(tasks []*Task) (result []persistedretry.Task) {
	for _, t := range tasks {
		result = append(result, t)
	}
	return result
}
