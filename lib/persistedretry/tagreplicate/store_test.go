package tagreplicate_test

import (
	"testing"

	"github.com/golang/mock/gomock"

	. "code.uber.internal/infra/kraken/lib/persistedretry/tagreplicate"
	"code.uber.internal/infra/kraken/mocks/lib/persistedretry/tagreplicate"
	"github.com/stretchr/testify/require"
)

func TestDeleteInvalidTasks(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	generator := mocktagreplicate.NewMockTaskGenerator(ctrl)
	store, source, cleanup := StoreFixture(generator)
	defer cleanup()

	task1 := TaskFixture()
	task2 := TaskFixture()
	store.MarkPending(task1)
	store.MarkPending(task2)
	store.MarkFailed(task2)

	generator.EXPECT().IsValid(gomock.Any()).Return(false).Times(2)

	newStore, err := NewStore(source, generator)
	require.NoError(err)

	tasks, err := newStore.GetPending()
	require.NoError(err)
	require.Empty(tasks)

	tasks, err = newStore.GetFailed()
	require.NoError(err)
	require.Empty(tasks)
}

func TestMarkPendingInsert(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	generator := mocktagreplicate.NewMockTaskGenerator(ctrl)
	store, _, cleanup := StoreFixture(generator)
	defer cleanup()

	task := TaskFixture()
	require.NoError(store.MarkPending(task))

	generator.EXPECT().Load(gomock.Any()).Return(nil)
	pendingTasks, err := store.GetPending()
	require.NoError(err)

	require.Equal(1, len(pendingTasks))
	EqualTask(t, *task, *pendingTasks[0].(*Task))
}

func TestMarkPendingReplace(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	generator := mocktagreplicate.NewMockTaskGenerator(ctrl)
	store, _, cleanup := StoreFixture(generator)
	defer cleanup()

	task := TaskFixture()
	require.NoError(store.MarkPending(task))
	task.Failures++
	require.NoError(store.MarkPending(task))

	generator.EXPECT().Load(gomock.Any()).Return(nil)
	pendingTasks, err := store.GetPending()
	require.NoError(err)

	require.Equal(1, len(pendingTasks))
	EqualTask(t, *task, *pendingTasks[0].(*Task))
}

func TestMarkFailed(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	generator := mocktagreplicate.NewMockTaskGenerator(ctrl)
	store, _, cleanup := StoreFixture(generator)
	defer cleanup()

	task := TaskFixture()
	require.NoError(store.MarkPending(task))
	require.NoError(store.MarkFailed(task))

	generator.EXPECT().Load(gomock.Any()).Return(nil)
	failedTasks, err := store.GetFailed()
	require.NoError(err)

	require.Equal(1, len(failedTasks))

	task.Failures++
	task.State = Failed
	EqualTask(t, *task, *failedTasks[0].(*Task))
}

func TestMarkFailedIgnore(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	generator := mocktagreplicate.NewMockTaskGenerator(ctrl)
	store, _, cleanup := StoreFixture(generator)
	defer cleanup()

	task := TaskFixture()
	require.NoError(store.MarkFailed(task))

	failedTasks, err := store.GetFailed()
	require.NoError(err)

	require.Equal(0, len(failedTasks))
}

func TestMarkDone(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	generator := mocktagreplicate.NewMockTaskGenerator(ctrl)
	store, _, cleanup := StoreFixture(generator)
	defer cleanup()

	task := TaskFixture()
	require.NoError(store.MarkPending(task))

	generator.EXPECT().Load(gomock.Any()).Return(nil)
	pendingTasks, err := store.GetPending()
	require.NoError(err)

	require.Equal(1, len(pendingTasks))
	EqualTask(t, *task, *pendingTasks[0].(*Task))

	require.NoError(store.MarkDone(task))
	pendingTasks, err = store.GetPending()
	require.NoError(err)

	require.Equal(0, len(pendingTasks))
}
