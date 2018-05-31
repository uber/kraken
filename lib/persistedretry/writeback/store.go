package writeback

import (
	"fmt"
	"time"

	"code.uber.internal/infra/kraken/lib/persistedretry"
	_ "code.uber.internal/infra/kraken/lib/persistedretry/writeback/migrations" // Adds migrations.
	"code.uber.internal/infra/kraken/utils/osutil"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // Loads SQL driver.
	"github.com/pressly/goose"
)

// Store stores writeback tasks.
type Store struct {
	db *sqlx.DB
}

// NewStore creates a new store from a sqlite source file.
func NewStore(source string) (*Store, error) {
	if err := osutil.EnsureFilePresent(source); err != nil {
		return nil, fmt.Errorf("ensure db source present: %s", err)
	}
	db, err := sqlx.Open("sqlite3", source)
	if err != nil {
		return nil, fmt.Errorf("open sqlite3: %s", err)
	}
	if err := goose.SetDialect("sqlite3"); err != nil {
		return nil, fmt.Errorf("set dialect as sqlite3: %s", err)
	}
	if err := goose.Up(db.DB, "."); err != nil {
		return nil, fmt.Errorf("perform db migration: %s", err)
	}
	return &Store{db}, nil
}

// Close closes s.
func (s *Store) Close() error {
	return s.db.Close()
}

// GetPending returns all pending tasks.
func (s *Store) GetPending() ([]persistedretry.Task, error) {
	return s.selectStatus("pending")
}

// GetFailed returns all failed tasks.
func (s *Store) GetFailed() ([]persistedretry.Task, error) {
	return s.selectStatus("failed")
}

// MarkPending marks r as pending.
func (s *Store) MarkPending(r persistedretry.Task) error {
	_, err := s.db.NamedExec(`
		INSERT OR REPLACE INTO writeback_task (
			namespace,
			digest,
			last_attempt,
			failures,
			delay,
			status
		) VALUES (
			:namespace,
			:digest,
			:last_attempt,
			:failures,
			:delay,
			"pending"
		)
	`, r.(*Task))
	return err
}

// MarkFailed marks r as failed.
func (s *Store) MarkFailed(r persistedretry.Task) error {
	t := r.(*Task)
	_, err := s.db.NamedExec(`
		INSERT OR REPLACE INTO writeback_task (
			namespace,
			digest,
			last_attempt,
			failures,
			delay,
			status
		) VALUES (
			:namespace,
			:digest,
			CURRENT_TIMESTAMP,
			COALESCE(
				(SELECT failures+1 FROM writeback_task
					WHERE namespace=:namespace AND digest=:digest),
				1),
			:delay,
			"failed"
		)
	`, t)
	if err != nil {
		return err
	}
	t.Failures++
	t.LastAttempt = time.Now()
	return nil
}

// MarkDone deletes r.
func (s *Store) MarkDone(r persistedretry.Task) error {
	_, err := s.db.NamedExec(`
		DELETE FROM writeback_task
		WHERE namespace=:namespace AND digest=:digest
	`, r.(*Task))
	return err
}

func (s *Store) selectStatus(status string) ([]persistedretry.Task, error) {
	var tasks []*Task
	err := s.db.Select(&tasks, `
		SELECT namespace, digest, created_at, last_attempt, failures, delay
		FROM writeback_task
		WHERE status=?
	`, status)
	if err != nil {
		return nil, err
	}
	var result []persistedretry.Task
	for _, t := range tasks {
		result = append(result, t)
	}
	return result, nil
}
