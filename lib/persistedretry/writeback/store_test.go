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

func TestMarkPending(t *testing.T) {
	require := require.New(t)

	store, cleanup := StoreFixture()
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.MarkPending(task))

	pending, err := store.GetPending()
	require.NoError(err)
	checkTasks(t, []*Task{task}, pending)

	require.True(pending[0].Ready())
}

func TestMarkPendingThenMarkFailed(t *testing.T) {
	require := require.New(t)

	store, cleanup := StoreFixture()
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.MarkPending(task))
	require.NoError(store.MarkFailed(task))

	pending, err := store.GetPending()
	require.NoError(err)
	require.Empty(pending)

	failed, err := store.GetFailed()
	require.NoError(err)
	checkTasks(t, []*Task{task}, failed)

	require.True(failed[0].Ready())
}

func TestMarkDone(t *testing.T) {
	require := require.New(t)

	store, cleanup := StoreFixture()
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.MarkPending(task))
	require.NoError(store.MarkDone(task))

	pending, err := store.GetPending()
	require.NoError(err)
	require.Empty(pending)

	failed, err := store.GetFailed()
	require.NoError(err)
	require.Empty(failed)
}

func TestMarkFailedForNewTaskWithDelay(t *testing.T) {
	require := require.New(t)

	store, cleanup := StoreFixture()
	defer cleanup()

	task := TaskFixture()
	task.Delay = 5 * time.Minute

	require.NoError(store.MarkFailed(task))

	failed, err := store.GetFailed()
	require.NoError(err)
	checkTasks(t, []*Task{task}, failed)

	require.False(failed[0].Ready())
}
