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
package tagreplication_test

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/jmoiron/sqlx"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/lib/persistedretry"
	. "github.com/uber/kraken/lib/persistedretry/tagreplication"
	"github.com/uber/kraken/localdb"
	mocktagreplication "github.com/uber/kraken/mocks/lib/persistedretry/tagreplication"
	"github.com/uber/kraken/utils/testutil"
)

type storeMocks struct {
	db *sqlx.DB
	rv *mocktagreplication.MockRemoteValidator
}

func newStoreMocks(t *testing.T) (*storeMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	db, c := localdb.Fixture()
	cleanup.Add(c)

	rv := mocktagreplication.NewMockRemoteValidator(ctrl)

	return &storeMocks{db, rv}, cleanup.Run
}

func (m *storeMocks) new() *Store {
	s, err := NewStore(m.db, m.rv)
	if err != nil {
		panic(err)
	}
	return s
}

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

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new()

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

func TestDeleteInvalidTasks(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new()

	task1 := TaskFixture()
	task2 := TaskFixture()

	store.AddPending(task1)
	store.AddFailed(task2)

	mocks.rv.EXPECT().Valid(task1.Tag, task1.Destination).Return(false)
	mocks.rv.EXPECT().Valid(task2.Tag, task2.Destination).Return(false)

	store = mocks.new()

	tasks, err := store.GetPending()
	require.NoError(err)
	require.Empty(tasks)

	tasks, err = store.GetFailed()
	require.NoError(err)
	require.Empty(tasks)
}

func TestAddPending(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new()

	task := TaskFixture()

	require.NoError(store.AddPending(task))

	checkPending(t, store, task)
}

func TestAddPendingTwiceReturnsErrTaskExists(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new()

	task := TaskFixture()

	require.NoError(store.AddPending(task))
	require.Equal(persistedretry.ErrTaskExists, store.AddPending(task))
}

func TestAddFailed(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new()

	task := TaskFixture()

	require.NoError(store.AddFailed(task))

	checkFailed(t, store, task)
}

func TestAddFailedTwiceReturnsErrTaskExists(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new()

	task := TaskFixture()

	require.NoError(store.AddFailed(task))
	require.Equal(persistedretry.ErrTaskExists, store.AddFailed(task))
}

func TestStateTransitions(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new()

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

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new()

	task := TaskFixture()

	require.Equal(persistedretry.ErrTaskNotFound, store.MarkPending(task))
	require.Equal(persistedretry.ErrTaskNotFound, store.MarkFailed(task))
}

func TestRemove(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new()

	task := TaskFixture()

	require.NoError(store.AddPending(task))

	checkPending(t, store, task)

	require.NoError(store.Remove(task))

	checkPending(t, store)
}

func TestDelay(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new()

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
