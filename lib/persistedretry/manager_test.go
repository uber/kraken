package persistedretry_test

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	. "code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/mocks/lib/persistedretry"
)

func TestMarkAllPendingAsFailedAtInit(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockpersistedretry.NewMockStore(ctrl)
	tasks := []Task{mockpersistedretry.NewMockTask(ctrl), mockpersistedretry.NewMockTask(ctrl)}

	gomock.InOrder(
		store.EXPECT().GetPending().Return(tasks, nil),
		store.EXPECT().MarkFailed(tasks[0]).Return(nil),
		store.EXPECT().MarkFailed(tasks[1]).Return(nil),
	)
	store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := New(testConfig(), tally.NoopScope, store)
	require.NoError(err)
	defer m.Close()

	time.Sleep(50 * time.Millisecond)
}

func TestAddTaskSuccess(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockpersistedretry.NewMockStore(ctrl)
	task := mockpersistedretry.NewMockTask(ctrl)
	config := testConfig()

	gomock.InOrder(
		store.EXPECT().GetPending().Return(nil, nil),
		store.EXPECT().MarkPending(task).Return(nil),
		task.EXPECT().Run().Return(nil),
		store.EXPECT().MarkDone(task).Return(nil),
	)
	store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := New(config, tally.NoopScope, store)
	require.NoError(err)
	defer m.Close()

	// wait until at least one worker starts so task will not fallback.
	time.Sleep(50 * time.Millisecond)
	addTaskHelper(require, m, task)
	time.Sleep(50 * time.Millisecond)
}

func TestAddTaskClosed(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockpersistedretry.NewMockStore(ctrl)
	task := mockpersistedretry.NewMockTask(ctrl)
	config := testConfig()

	store.EXPECT().GetPending().Return(nil, nil)

	m, err := New(config, tally.NoopScope, store)
	require.NoError(err)
	m.Close()

	require.Equal(ErrManagerClosed, m.Add(task))
}

func TestAddTaskFail(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockpersistedretry.NewMockStore(ctrl)
	task := mockpersistedretry.NewMockTask(ctrl)
	config := testConfig()

	gomock.InOrder(
		store.EXPECT().GetPending().Return(nil, nil),
		store.EXPECT().MarkPending(task).Return(nil),
		task.EXPECT().Run().Return(errors.New("task failed")),
		store.EXPECT().MarkFailed(task).Return(nil),
	)
	store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := New(config, tally.NoopScope, store)
	require.NoError(err)
	defer m.Close()

	// wait until at least one worker starts so task will not fallback.
	time.Sleep(50 * time.Millisecond)
	addTaskHelper(require, m, task)
	time.Sleep(50 * time.Millisecond)
}

func TestAddTaskFallback(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockpersistedretry.NewMockStore(ctrl)
	config := testConfig()
	task1 := newTestTask()
	task2 := mockpersistedretry.NewMockTask(ctrl)

	store.EXPECT().GetPending().Return(nil, nil)
	store.EXPECT().MarkPending(task1).Return(nil)
	store.EXPECT().MarkPending(task2).Return(nil)
	store.EXPECT().MarkFailed(task2).Return(nil)
	store.EXPECT().MarkDone(task1).Return(nil)
	store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := New(config, tally.NoopScope, store)
	require.NoError(err)
	defer m.Close()

	// wait until at least one worker starts so task will not fallback.
	time.Sleep(50 * time.Millisecond)
	addTaskHelper(require, m, task1)
	addTaskHelper(require, m, task2)
	task1.Finish()
	time.Sleep(50 * time.Millisecond)
}

func TestNoTask(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockpersistedretry.NewMockStore(ctrl)
	config := testConfig()

	store.EXPECT().GetPending().Return(nil, nil)
	store.EXPECT().GetFailed().Return([]Task{}, nil).MinTimes(1)

	m, err := New(config, tally.NoopScope, store)
	require.NoError(err)
	defer m.Close()
	time.Sleep(50 * time.Millisecond)
}

func TestRetry(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockpersistedretry.NewMockStore(ctrl)
	task := mockpersistedretry.NewMockTask(ctrl)
	config := testConfig()

	gomock.InOrder(
		store.EXPECT().GetPending().Return(nil, nil).MinTimes(1),
		store.EXPECT().GetFailed().Return([]Task{task}, nil),
		store.EXPECT().MarkPending(task),
		task.EXPECT().Run().Return(nil),
		store.EXPECT().MarkDone(task).Return(nil),
	)
	store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := New(config, tally.NoopScope, store)
	require.NoError(err)
	defer m.Close()
	time.Sleep(50 * time.Millisecond)
}

func TestRetryFallback(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockpersistedretry.NewMockStore(ctrl)
	task1 := newTestTask()
	task2 := mockpersistedretry.NewMockTask(ctrl)
	config := testConfig()

	gomock.InOrder(
		store.EXPECT().GetPending().Return(nil, nil).MinTimes(1),
		// Make sure the first GetFailed returns two tasks.
		store.EXPECT().GetFailed().Return([]Task{task1, task2}, nil),
		store.EXPECT().MarkPending(task1),
		store.EXPECT().MarkDone(task1).Return(nil),
	)
	// task2 should fallback.
	store.EXPECT().MarkPending(task2).Return(nil)
	store.EXPECT().MarkFailed(task2).Return(nil)
	store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := New(config, tally.NoopScope, store)
	require.NoError(err)
	defer m.Close()
	time.Sleep(time.Second)
	task1.Finish()
	time.Sleep(time.Second)
}
