package tagreplication

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // tagreplication.Store is based on sqlite3
	"github.com/pressly/goose"

	"code.uber.internal/infra/kraken/lib/persistedretry"
	_ "code.uber.internal/infra/kraken/lib/persistedretry/tagreplication/migrations" // registry db migrations
	"code.uber.internal/infra/kraken/utils/osutil"
)

// Store stores tags to be replicated asynchronously.
type Store struct {
	db *sqlx.DB
}

// NewStore creates a new Store.
func NewStore(source string, rv RemoteValidator) (*Store, error) {
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

	s := &Store{db}

	if err := s.deleteInvalidTasks(rv); err != nil {
		return nil, fmt.Errorf("delete invalid tasks: %s", err)
	}

	return s, nil
}

// Close closes the store.
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

// MarkPending inserts a tag in db.
func (s *Store) MarkPending(r persistedretry.Task) error {
	_, err := s.db.NamedExec(`
		INSERT OR REPLACE INTO replicate_tag_task (
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
			"pending"
		)`, r.(*Task))
	return err
}

// MarkFailed marks a task as failed.
func (s *Store) MarkFailed(r persistedretry.Task) error {
	t := r.(*Task)
	_, err := s.db.NamedExec(`
		INSERT OR REPLACE INTO replicate_tag_task (
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
			CURRENT_TIMESTAMP,
			COALESCE(
				(SELECT failures+1 FROM replicate_tag_task
					WHERE tag=:tag AND destination=:destination),
				1),
			:delay,
			"failed"
		)`, t)
	if err != nil {
		return err
	}
	t.Failures++
	t.LastAttempt = time.Now()
	return nil
}

// MarkDone deletes a tag in db.
func (s *Store) MarkDone(r persistedretry.Task) error {
	return s.delete(r)
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
