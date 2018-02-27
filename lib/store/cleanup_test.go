package store

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store/base"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func fileOpFixture(clk clock.Clock) (base.FileState, base.FileOp, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	dir, err := ioutil.TempDir("/tmp", "cleanup_test")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(dir) })

	state := agentFileState{dir}

	store, err := base.NewLocalFileStore(clk)
	if err != nil {
		panic(err)
	}

	return state, store.NewFileOp().AcceptState(state), cleanup.Run
}

func TestCleanupManagerDeleteIdleFiles(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	clk.Set(time.Now())

	m := newCleanupManager(clk)

	tti := 24 * time.Hour

	state, op, cleanup := fileOpFixture(clk)
	defer cleanup()

	var names []string
	for i := 0; i < 100; i++ {
		names = append(names, core.DigestFixture().Hex())
	}

	idle := names[:50]
	for _, name := range idle {
		require.NoError(op.CreateFile(name, state, 0))
	}

	clk.Add(tti + 1)

	active := names[50:]
	for _, name := range active {
		require.NoError(op.CreateFile(name, state, 0))
	}

	require.NoError(m.deleteIdleFiles(op, tti))

	for _, name := range idle {
		_, err := op.GetFileStat(name)
		require.True(os.IsNotExist(err))
	}
	for _, name := range active {
		_, err := op.GetFileStat(name)
		require.NoError(err)
	}
}

func TestCleanupManagerAddJob(t *testing.T) {
	require := require.New(t)

	clk := clock.New()

	m := newCleanupManager(clk)
	defer m.stop()

	state, op, cleanup := fileOpFixture(clk)
	defer cleanup()

	config := CleanupConfig{
		Interval: time.Second,
		TTI:      time.Second,
	}
	m.addJob(config, op)

	name := "test_file"

	require.NoError(op.CreateFile(name, state, 0))

	time.Sleep(2 * time.Second)

	_, err := op.GetFileStat(name)
	require.True(os.IsNotExist(err))
}
