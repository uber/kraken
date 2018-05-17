package persistedretry_test

import (
	"sync"
	"time"

	"github.com/stretchr/testify/require"

	. "code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/utils/randutil"
)

func addTaskHelper(r *require.Assertions, m Manager, t Task) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.NoError(m.Add(t))
	}()
	wg.Wait()
}

func testConfig() Config {
	return Config{
		NumWorkers:        1,
		NumRetryWorkers:   1,
		TaskChanSize:      0,
		RetryChanSize:     0,
		TaskInterval:      5 * time.Millisecond,
		RetryInterval:     10 * time.Millisecond,
		RetryTaskInterval: 5 * time.Millisecond,
	}
}

var _ Task = (*testTask)(nil)

type testTask struct {
	c  chan struct{}
	id string
}

func newTestTask() *testTask {
	return &testTask{make(chan struct{}, 0), string(randutil.Text(4))}
}

func (t *testTask) Run() error {
	<-t.c
	return nil
}

func (t *testTask) String() string {
	return t.id
}

func (t *testTask) Finish() {
	t.c <- struct{}{}
}
