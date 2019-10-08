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
package persistedretry_test

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	. "github.com/uber/kraken/lib/persistedretry"
	"github.com/uber/kraken/mocks/lib/persistedretry"
)

func waitForWorkers() {
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
}

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
			IncomingBuffer:      0,
			RetryBuffer:         0,
			NumIncomingWorkers:  1,
			NumRetryWorkers:     1,
			MaxTaskThroughput:   5 * time.Millisecond,
			RetryInterval:       100 * time.Millisecond,
			PollRetriesInterval: 5 * time.Millisecond,
			Testing:             true,
		},
		store:    mockpersistedretry.NewMockStore(ctrl),
		executor: mockpersistedretry.NewMockExecutor(ctrl),
	}, ctrl.Finish
}

func (m *managerMocks) new() (Manager, error) {
	m.executor.EXPECT().Name().Return("mock executor")
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
		task.EXPECT().Ready().Return(true),
		mocks.store.EXPECT().AddPending(task).Return(nil),
		mocks.executor.EXPECT().Exec(task).Return(nil),
		mocks.store.EXPECT().Remove(task).Return(nil),
	)

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	waitForWorkers()

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
		task.EXPECT().Ready().Return(true),
		mocks.store.EXPECT().AddPending(task).Return(nil),
		mocks.executor.EXPECT().Exec(task).Return(errors.New("task failed")),
		mocks.store.EXPECT().MarkFailed(task).Return(nil),
		task.EXPECT().GetFailures().Return(1),
		task.EXPECT().Tags().Return(nil),
	)

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	waitForWorkers()

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
		task1.EXPECT().Ready().Return(true),
		mocks.store.EXPECT().AddPending(task1).Return(nil),
		mocks.executor.EXPECT().Exec(task1).DoAndReturn(func(Task) error {
			<-task1Done
			return nil
		}),
		mocks.store.EXPECT().Remove(task1).Return(nil),
	)

	gomock.InOrder(
		task2.EXPECT().Ready().Return(true),
		mocks.store.EXPECT().AddPending(task2).Return(nil),
		mocks.store.EXPECT().MarkFailed(task2).Return(nil),
	)

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	waitForWorkers()

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
		task.EXPECT().Ready().Return(true),
		task.EXPECT().GetLastAttempt().Return(time.Time{}),
		mocks.store.EXPECT().MarkPending(task),
		mocks.executor.EXPECT().Exec(task).Return(nil),
		mocks.store.EXPECT().Remove(task).Return(nil),
	)
	mocks.store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	time.Sleep(50 * time.Millisecond)
}

func TestManagerRetriesSkipsNotReadyTasks(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	task := mocks.task()

	gomock.InOrder(
		mocks.store.EXPECT().GetPending().Return(nil, nil).MinTimes(1),
		mocks.store.EXPECT().GetFailed().Return([]Task{task}, nil),
		task.EXPECT().Ready().Return(false),
	)
	mocks.store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	time.Sleep(50 * time.Millisecond)
}

func TestManagerRetriesSkipsRecentlyAttemptedTasks(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	task := mocks.task()

	gomock.InOrder(
		mocks.store.EXPECT().GetPending().Return(nil, nil).MinTimes(1),
		mocks.store.EXPECT().GetFailed().Return([]Task{task}, nil),
		task.EXPECT().Ready().Return(true),
		task.EXPECT().GetLastAttempt().Return(time.Now()),
	)
	mocks.store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	time.Sleep(50 * time.Millisecond)
}

func TestManagerAddNotReadyTaskMarksAsFailed(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	task := mocks.task()

	mocks.store.EXPECT().GetPending().Return(nil, nil)

	gomock.InOrder(
		task.EXPECT().Ready().Return(false),
		mocks.store.EXPECT().AddFailed(task).Return(nil),
	)

	mocks.store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	waitForWorkers()

	require.NoError(m.Add(task))
}

func TestManagerNoopsOnFailedTaskConflicts(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	task := mocks.task()

	mocks.store.EXPECT().GetPending().Return(nil, nil)

	gomock.InOrder(
		task.EXPECT().Ready().Return(false),
		mocks.store.EXPECT().AddFailed(task).Return(ErrTaskExists),
	)

	mocks.store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	waitForWorkers()

	require.NoError(m.Add(task))
}

func TestManagerNoopsOnPendingTaskConflicts(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	task := mocks.task()

	mocks.store.EXPECT().GetPending().Return(nil, nil)

	gomock.InOrder(
		task.EXPECT().Ready().Return(true),
		mocks.store.EXPECT().AddPending(task).Return(ErrTaskExists),
	)

	mocks.store.EXPECT().GetFailed().Return(nil, nil).AnyTimes()

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	waitForWorkers()

	require.NoError(m.Add(task))
}

func TestManagerExec(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newManagerMocks(t)
	defer cleanup()

	task := mocks.task()

	mocks.store.EXPECT().GetPending().Return(nil, nil)

	m, err := mocks.new()
	require.NoError(err)
	defer m.Close()

	mocks.executor.EXPECT().Exec(task).Return(nil)

	require.NoError(m.SyncExec(task))
}
