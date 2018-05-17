package persistedretry_test

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	. "code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/mocks/lib/persistedretry"
)

type managerMocks struct {
	ctrl     *gomock.Controller
	config   Config
	store    *mockpersistedretry.MockStore
	executor *mockpersistedretry.MockExecutor
}

func newManagerMocks(t *testing.T) (*managerMocks, func()) {
	ctrl := gomock.NewController(t)
	return &managerMocks{
		ctrl: ctrl,
		config: Config{
			NumWorkers:        1,
			NumRetryWorkers:   1,
			TaskChanSize:      0,
			RetryChanSize:     0,
			TaskInterval:      5 * time.Millisecond,
			RetryInterval:     10 * time.Millisecond,
			RetryTaskInterval: 5 * time.Millisecond,
			Testing:           true,
		},
		store:    mockpersistedretry.NewMockStore(ctrl),
		executor: mockpersistedretry.NewMockExecutor(ctrl),
	}, ctrl.Finish
}

func (m *managerMocks) new() (Manager, error) {
	return NewManager(m.config, tally.NoopScope, m.store, m.executor)
}

func (m *managerMocks) task() *mockpersistedretry.MockTask {
	return mockpersistedretry.NewMockTask(m.ctrl)
}

func TestNewManagerMarksAllPendingTasksAsFailed(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	tasks := []Task{mocks.task(), mocks.task()}

	mocks.store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	gomock.InOrder(
		mocks.store.EXPECT().GetPending().Return(tasks, nil),
		mocks.store.EXPECT().MarkFailed(tasks[0]).Return(nil),
		mocks.store.EXPECT().MarkFailed(tasks[1]).Return(nil),
	)

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	time.Sleep(50 * time.Millisecond)
}

func TestManagerAddTaskSuccess(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	task := mocks.task()

	mocks.store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	gomock.InOrder(
		mocks.store.EXPECT().GetPending().Return(nil, nil),
		mocks.store.EXPECT().MarkPending(task).Return(nil),
		mocks.executor.EXPECT().Exec(task).Return(nil),
		mocks.store.EXPECT().MarkDone(task).Return(nil),
	)

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	// Let workers start.
	runtime.Gosched()

	require.NoError(m.Add(task))

	time.Sleep(50 * time.Millisecond)
}

func TestManagerAddTaskClosed(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	mocks.store.EXPECT().GetPending().Return(nil, nil)

	m, err := mocks.new()
	require.NoError(err)

	m.Close()

	require.Equal(ErrManagerClosed, m.Add(mocks.task()))
}

func TestManagerAddTaskFail(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	task := mocks.task()

	mocks.store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	gomock.InOrder(
		mocks.store.EXPECT().GetPending().Return(nil, nil),
		mocks.store.EXPECT().MarkPending(task).Return(nil),
		mocks.executor.EXPECT().Exec(task).Return(errors.New("task failed")),
		mocks.store.EXPECT().MarkFailed(task).Return(nil),
	)

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	// Let workers start.
	runtime.Gosched()

	require.NoError(m.Add(task))

	time.Sleep(50 * time.Millisecond)
}

func TestManagerAddTaskFallbackWhenWorkersBusy(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	task1 := mocks.task()
	task2 := mocks.task()

	task1Done := make(chan bool)

	mocks.store.EXPECT().GetPending().Return(nil, nil)
	mocks.store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	gomock.InOrder(
		mocks.store.EXPECT().MarkPending(task1).Return(nil),
		mocks.executor.EXPECT().Exec(task1).DoAndReturn(func(Task) error {
			<-task1Done
			return nil
		}),
		mocks.store.EXPECT().MarkDone(task1).Return(nil),
	)

	gomock.InOrder(
		mocks.store.EXPECT().MarkPending(task2).Return(nil),
		mocks.store.EXPECT().MarkFailed(task2).Return(nil),
	)

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	// Let workers start.
	runtime.Gosched()

	// First task blocks, so the only worker is busy when we add second task, which
	// should then fallback to failed.
	require.NoError(m.Add(task1))
	require.NoError(m.Add(task2))

	task1Done <- true

	time.Sleep(50 * time.Millisecond)
}

func TestManagerRetriesFailedTasks(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	task := mocks.task()

	gomock.InOrder(
		mocks.store.EXPECT().GetPending().Return(nil, nil).MinTimes(1),
		mocks.store.EXPECT().GetFailed().Return([]Task{task}, nil),
		mocks.store.EXPECT().MarkPending(task),
		mocks.executor.EXPECT().Exec(task).Return(nil),
		mocks.store.EXPECT().MarkDone(task).Return(nil),
	)
	mocks.store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	time.Sleep(50 * time.Millisecond)
}

func TestManagerFailedTaskRetryFallbackWhenWorkersBusy(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	task1 := mocks.task()
	task2 := mocks.task()

	task1Done := make(chan bool)

	mocks.store.EXPECT().GetPending().Return(nil, nil)

	gomock.InOrder(
		mocks.store.EXPECT().GetFailed().Return([]Task{task1, task2}, nil),
		mocks.store.EXPECT().GetFailed().Return(nil, nil).AnyTimes(),
	)

	gomock.InOrder(
		mocks.store.EXPECT().MarkPending(task1),
		mocks.executor.EXPECT().Exec(task1).DoAndReturn(func(Task) error {
			<-task1Done
			return nil
		}),
		mocks.store.EXPECT().MarkDone(task1).Return(nil),
	)

	gomock.InOrder(
		mocks.store.EXPECT().MarkPending(task2).Return(nil),
		mocks.store.EXPECT().MarkFailed(task2).Return(nil),
	)

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	time.Sleep(time.Second)

	task1Done <- true

	time.Sleep(time.Second)
}
