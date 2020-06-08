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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/lib/persistedretry"
	"github.com/uber/kraken/localdb"
)

func checkTask(t *testing.T, expected *Task, result persistedretry.Task) {
	t.Helper()

	expectedCopy := *expected
	resultCopy := *(result.(*Task))

	require.InDelta(t, expectedCopy.CreatedAt.Unix(), resultCopy.CreatedAt.Unix(), 1)
	expectedCopy.CreatedAt = time.Time{}
	resultCopy.CreatedAt = time.Time{}

	require.InDelta(t, expectedCopy.LastAttempt.Unix(), resultCopy.LastAttempt.Unix(), 1)
	expectedCopy.LastAttempt = time.Time{}
	resultCopy.LastAttempt = time.Time{}

	require.Equal(t, expectedCopy, resultCopy)
}

func checkTasks(t *testing.T, expected []*Task, result []persistedretry.Task) {
	t.Helper()

	require.Equal(t, len(expected), len(result))

	for i := 0; i < len(expected); i++ {
		checkTask(t, expected[i], result[i])
	}
}

func checkPending(t *testing.T, store *Store, expected ...*Task) {
	t.Helper()

	result, err := store.GetPending()
	require.NoError(t, err)
	checkTasks(t, expected, result)
}

func checkFailed(t *testing.T, store *Store, expected ...*Task) {
	t.Helper()

	result, err := store.GetFailed()
	require.NoError(t, err)
	checkTasks(t, expected, result)
}

func TestDatabaseNotLocked(t *testing.T) {
	require := require.New(t)

	db, cleanup := localdb.Fixture()
	defer cleanup()

	store := NewStore(db)

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.GetFailed()
			require.NoError(err)
			require.NoError(store.AddPending(TaskFixture()))
		}()

	}
	wg.Wait()
}

func TestAddPending(t *testing.T) {
	require := require.New(t)

	db, cleanup := localdb.Fixture()
	defer cleanup()

	store := NewStore(db)

	task := TaskFixture()

	require.NoError(store.AddPending(task))

	checkPending(t, store, task)
}

func TestAddPendingTwiceReturnsErrTaskExists(t *testing.T) {
	require := require.New(t)

	db, cleanup := localdb.Fixture()
	defer cleanup()

	store := NewStore(db)

	task := TaskFixture()

	require.NoError(store.AddPending(task))
	require.Equal(persistedretry.ErrTaskExists, store.AddPending(task))
}

func TestAddFailed(t *testing.T) {
	require := require.New(t)

	db, cleanup := localdb.Fixture()
	defer cleanup()

	store := NewStore(db)

	task := TaskFixture()

	require.NoError(store.AddFailed(task))

	checkFailed(t, store, task)
}

func TestAddFailedTwiceReturnsErrTaskExists(t *testing.T) {
	require := require.New(t)

	db, cleanup := localdb.Fixture()
	defer cleanup()

	store := NewStore(db)

	task := TaskFixture()

	require.NoError(store.AddFailed(task))
	require.Equal(persistedretry.ErrTaskExists, store.AddFailed(task))
}

func TestStateTransitions(t *testing.T) {
	require := require.New(t)

	db, cleanup := localdb.Fixture()
	defer cleanup()

	store := NewStore(db)

	task := TaskFixture()

	require.NoError(store.AddPending(task))
	checkPending(t, store, task)
	checkFailed(t, store)

	require.NoError(store.MarkFailed(task))
	checkPending(t, store)
	checkFailed(t, store, task)

	require.NoError(store.MarkPending(task))
	checkPending(t, store, task)
	checkFailed(t, store)
}

func TestMarkTaskNotFound(t *testing.T) {
	require := require.New(t)

	db, cleanup := localdb.Fixture()
	defer cleanup()

	store := NewStore(db)

	task := TaskFixture()

	require.Equal(persistedretry.ErrTaskNotFound, store.MarkPending(task))
	require.Equal(persistedretry.ErrTaskNotFound, store.MarkFailed(task))
}

func TestRemove(t *testing.T) {
	require := require.New(t)

	db, cleanup := localdb.Fixture()
	defer cleanup()

	store := NewStore(db)

	task := TaskFixture()

	require.NoError(store.AddPending(task))

	checkPending(t, store, task)

	require.NoError(store.Remove(task))

	checkPending(t, store)
}

func TestDelay(t *testing.T) {
	require := require.New(t)

	db, cleanup := localdb.Fixture()
	defer cleanup()

	store := NewStore(db)

	task1 := TaskFixture()
	task1.Delay = 5 * time.Minute

	task2 := TaskFixture()
	task2.Delay = 0

	require.NoError(store.AddPending(task1))
	require.NoError(store.AddPending(task2))

	pending, err := store.GetPending()
	require.NoError(err)
	checkTasks(t, []*Task{task1, task2}, pending)

	require.False(pending[0].Ready())
	require.True(pending[1].Ready())
}

func TestFind(t *testing.T) {
	require := require.New(t)

	db, cleanup := localdb.Fixture()
	defer cleanup()

	store := NewStore(db)

	task1 := TaskFixture()
	task2 := TaskFixture()

	require.NoError(store.AddPending(task1))
	require.NoError(store.AddPending(task2))

	result, err := store.Find(NewNameQuery(task1.Name))
	require.NoError(err)
	checkTasks(t, []*Task{task1}, result)
}

func TestFindEmpty(t *testing.T) {
	require := require.New(t)

	db, cleanup := localdb.Fixture()
	defer cleanup()

	store := NewStore(db)

	require.NoError(store.AddPending(TaskFixture()))

	result, err := store.Find(NewNameQuery("nonexistent name"))
	require.NoError(err)
	require.Empty(result)
}
