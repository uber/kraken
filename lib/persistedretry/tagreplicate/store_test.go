package tagreplicate_test

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	. "code.uber.internal/infra/kraken/lib/persistedretry/tagreplicate"
	"code.uber.internal/infra/kraken/mocks/lib/persistedretry/tagreplicate"
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

func TestDeleteInvalidTasks(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rv := mocktagreplicate.NewMockRemoteValidator(ctrl)

	store, source, cleanup := StoreFixture(rv)
	defer cleanup()

	task1 := TaskFixture()
	task2 := TaskFixture()

	store.MarkPending(task1)

	store.MarkPending(task2)
	store.MarkFailed(task2)

	require.NoError(store.Close())

	rv.EXPECT().Valid(task1.Tag, task1.Destination).Return(false)
	rv.EXPECT().Valid(task2.Tag, task2.Destination).Return(false)

	store, err := NewStore(source, rv)
	require.NoError(err)

	tasks, err := store.GetPending()
	require.NoError(err)
	require.Empty(tasks)

	tasks, err = store.GetFailed()
	require.NoError(err)
	require.Empty(tasks)
}

func TestMarkPending(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rv := mocktagreplicate.NewMockRemoteValidator(ctrl)

	store, _, cleanup := StoreFixture(rv)
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.MarkPending(task))

	pending, err := store.GetPending()
	require.NoError(err)
	checkTasks(t, []*Task{task}, pending)
}

func TestMarkPendingTwiceReplacesTask(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rv := mocktagreplicate.NewMockRemoteValidator(ctrl)

	store, _, cleanup := StoreFixture(rv)
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.MarkPending(task))

	task.Digest = core.DigestFixture()

	require.NoError(store.MarkPending(task))

	pending, err := store.GetPending()
	require.NoError(err)
	checkTasks(t, []*Task{task}, pending)
}

func TestMarkFailed(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rv := mocktagreplicate.NewMockRemoteValidator(ctrl)

	store, _, cleanup := StoreFixture(rv)
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.MarkPending(task))
	require.NoError(store.MarkFailed(task))

	failed, err := store.GetFailed()
	require.NoError(err)
	checkTasks(t, []*Task{task}, failed)
}

func TestMarkFailedIgnoresIfNotPending(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rv := mocktagreplicate.NewMockRemoteValidator(ctrl)

	store, _, cleanup := StoreFixture(rv)
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.MarkFailed(task))

	failed, err := store.GetFailed()
	require.NoError(err)
	require.Empty(failed)
}

func TestMarkDone(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rv := mocktagreplicate.NewMockRemoteValidator(ctrl)

	store, _, cleanup := StoreFixture(rv)
	defer cleanup()

	task := TaskFixture()

	require.NoError(store.MarkPending(task))

	pending, err := store.GetPending()
	require.NoError(err)
	checkTasks(t, []*Task{task}, pending)

	require.NoError(store.MarkDone(task))

	pending, err = store.GetPending()
	require.NoError(err)
	require.Empty(pending)
}
