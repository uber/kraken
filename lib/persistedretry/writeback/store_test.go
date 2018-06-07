package writeback

import (
	"testing"
	"time"

	"code.uber.internal/infra/kraken/lib/persistedretry"
	"github.com/stretchr/testify/require"
)

func checkTask(t *testing.T, expected *Task, result persistedretry.Task) {
	t.Helper()

	expectedCopy := *expected
	resultCopy := *(result.(*Task))

	require.Equal(t, expectedCopy.CreatedAt.Unix(), resultCopy.CreatedAt.Unix())
	expectedCopy.CreatedAt = time.Time{}
	resultCopy.CreatedAt = time.Time{}

	require.Equal(t, expectedCopy.LastAttempt.Unix(), resultCopy.LastAttempt.Unix())
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

func TestAddPending(t *testing.T) {
	require := require.New(t)

	store, cleanup := StoreFixture()
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.AddPending(task))

	checkPending(t, store, task)
}

func TestAddPendingTwiceReturnsErrTaskExists(t *testing.T) {
	require := require.New(t)

	store, cleanup := StoreFixture()
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.AddPending(task))
	require.Equal(persistedretry.ErrTaskExists, store.AddPending(task))
}

func TestAddFailed(t *testing.T) {
	require := require.New(t)

	store, cleanup := StoreFixture()
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.AddFailed(task))

	checkFailed(t, store, task)
}

func TestAddFailedTwiceReturnsErrTaskExists(t *testing.T) {
	require := require.New(t)

	store, cleanup := StoreFixture()
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.AddFailed(task))
	require.Equal(persistedretry.ErrTaskExists, store.AddFailed(task))
}

func TestStateTransitions(t *testing.T) {
	require := require.New(t)

	store, cleanup := StoreFixture()
	defer cleanup()

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

	store, cleanup := StoreFixture()
	defer cleanup()

	task := TaskFixture()

	require.Equal(persistedretry.ErrTaskNotFound, store.MarkPending(task))
	require.Equal(persistedretry.ErrTaskNotFound, store.MarkFailed(task))
}

func TestRemove(t *testing.T) {
	require := require.New(t)

	store, cleanup := StoreFixture()
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.AddPending(task))

	checkPending(t, store, task)

	require.NoError(store.Remove(task))

	checkPending(t, store)
}

func TestDelay(t *testing.T) {
	require := require.New(t)

	store, cleanup := StoreFixture()
	defer cleanup()

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
