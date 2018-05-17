package tagreplicate

import (
	"fmt"
	"os"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // tagreplicate.Store is based on sqlite3
	"github.com/pressly/goose"

	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/utils/log"
)

var _ persistedretry.Store = (*Store)(nil)

// Store stores tags to be replicated asynchronously.
type Store struct {
	db        *sqlx.DB
	generator TaskGenerator
}

// NewStore creates a new Store.
func NewStore(
	source string,
	generator TaskGenerator) (*Store, error) {
	// Create source file if not exist.
	_, err := os.Stat(source)
	if os.IsNotExist(err) {
		f, err := os.Create(source)
		if err != nil {
			return nil, fmt.Errorf("create source file: %s", err)
		}
		f.Close()
	} else if err != nil {
		return nil, fmt.Errorf("stat source file: %s", err)
	}

	db, err := sqlx.Open("sqlite3", source)
	if err != nil {
		return nil, fmt.Errorf("open sqlite3: %s", err)
	}

	if err := goose.SetDialect("sqlite3"); err != nil {
		return nil, fmt.Errorf("set dialect as sqlite3: %s", err)
	}

	if err := goose.Up(db.DB, "./migrations"); err != nil {
		return nil, fmt.Errorf("perform db migration: %s", err)
	}

	store := &Store{db, generator}
	if err := store.deleteInvalidTasks(); err != nil {
		return nil, err
	}
	return store, nil
}

// deleteInvalidTasks deletes replication tasks that are not .
func (ts *Store) deleteInvalidTasks() error {
	tasks := []*Task{}
	if err := ts.db.Select(&tasks, `SELECT * FROM replicate_tag_task`); err != nil {
		return err
	}

	for _, t := range tasks {
		if !ts.generator.IsValid(*t) {
			if err := ts.delete(t); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetPending returns all replicate tags which have failed=0.
func (ts *Store) GetPending() ([]persistedretry.Task, error) {
	tasks := []*Task{}
	if err := ts.db.Select(&tasks, fmt.Sprintf(`SELECT * FROM replicate_tag_task WHERE state=%q`, Pending)); err != nil {
		return nil, err
	}

	var results []persistedretry.Task
	for _, t := range tasks {
		if err := ts.generator.Load(t); err != nil {
			log.Errorf("Failed to create task: %s", err)
			continue
		}
		results = append(results, t)
	}

	return results, nil
}

// GetFailed returns all tags in retries.
func (ts *Store) GetFailed() ([]persistedretry.Task, error) {
	tasks := []*Task{}
	if err := ts.db.Select(&tasks, fmt.Sprintf(`SELECT * FROM replicate_tag_task WHERE state=%q`, Failed)); err != nil {
		return nil, err
	}

	var results []persistedretry.Task
	for _, t := range tasks {
		if err := ts.generator.Load(t); err != nil {
			log.Errorf("Failed to create task: %s", err)
			continue
		}
		results = append(results, t)
	}

	return results, nil
}

// MarkPending inserts a tag in db.
func (ts *Store) MarkPending(r persistedretry.Task) error {
	_, err := ts.db.NamedExec(
		fmt.Sprintf(`INSERT OR REPLACE INTO replicate_tag_task (
			name,
			digest,
			dependencies,
			destination,
			created_at,
			last_attempt,
			state,
			failures) VALUES (
			:name,
			:digest,
			:dependencies,
			:destination,
			:created_at,
			:last_attempt,
			%q,
			:failures)`, Pending),
		r.(*Task))
	return err
}

// MarkFailed set failed=1 in a tag.
func (ts *Store) MarkFailed(r persistedretry.Task) error {
	_, err := ts.db.NamedExec(
		fmt.Sprintf(
			`UPDATE replicate_tag_task SET failures=failures+1,state=%q 
		WHERE name=:name AND destination=:destination`, Failed),
		r.(*Task))
	return err
}

// MarkDone deletes a tag in db.
func (ts *Store) MarkDone(r persistedretry.Task) error {
	return ts.delete(r)
}

// delete deletes a tag in db.
func (ts *Store) delete(r persistedretry.Task) error {
	_, err := ts.db.NamedExec(
		`DELETE FROM replicate_tag_task 
		WHERE name=:name AND destination=:destination`,
		r.(*Task))
	return err
}
