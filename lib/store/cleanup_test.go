package store

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store/base"
	"code.uber.internal/infra/kraken/lib/store/metadata"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
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

func TestCleanupManagerAddJob(t *testing.T) {
	require := require.New(t)

	clk := clock.New()

	m := newCleanupManager(clk, tally.NoopScope)
	defer m.stop()

	state, op, cleanup := fileOpFixture(clk)
	defer cleanup()

	config := CleanupConfig{
		Interval: time.Second,
		TTI:      time.Second,
	}
	m.addJob("test_cleanup", config, op)

	name := "test_file"

	require.NoError(op.CreateFile(name, state, 0))

	time.Sleep(2 * time.Second)

	_, err := op.GetFileStat(name)
	require.True(os.IsNotExist(err))
}

func TestCleanupManagerDeleteIdleFiles(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	clk.Set(time.Now())
	tti := 6 * time.Hour
	ttl := 24 * time.Hour

	scope := tally.NoopScope
	gauge := scope.Gauge("test")

	m := newCleanupManager(clk, scope)

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

	require.NoError(m.deleteIdleOrExpiredFiles(op, tti, ttl, gauge))

	for _, name := range idle {
		_, err := op.GetFileStat(name)
		require.True(os.IsNotExist(err))
	}
	for _, name := range active {
		_, err := op.GetFileStat(name)
		require.NoError(err)
	}
}

func TestCleanupManagerDeleteExpiredFiles(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	clk.Set(time.Now())
	tti := 6 * time.Hour
	ttl := 24 * time.Hour

	scope := tally.NoopScope
	gauge := scope.Gauge("test")

	m := newCleanupManager(clk, scope)

	state, op, cleanup := fileOpFixture(clk)
	defer cleanup()

	var names []string
	for i := 0; i < 10; i++ {
		names = append(names, core.DigestFixture().Hex())
	}
	for _, name := range names {
		require.NoError(op.CreateFile(name, state, 0))
	}

	require.NoError(m.deleteIdleOrExpiredFiles(op, tti, ttl, gauge))
	for _, name := range names {
		_, err := op.GetFileStat(name)
		require.NoError(err)
	}

	clk.Add(ttl + 1)

	require.NoError(m.deleteIdleOrExpiredFiles(op, tti, ttl, gauge))
	for _, name := range names {
		_, err := op.GetFileStat(name)
		require.True(os.IsNotExist(err))
	}
}

func TestCleanupManagerSkipsPersistedFiles(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	clk.Set(time.Now())
	tti := 48 * time.Hour
	ttl := 24 * time.Hour

	scope := tally.NoopScope
	gauge := scope.Gauge("test")

	m := newCleanupManager(clk, scope)

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

	persisted := names[50:]
	for _, name := range persisted {
		require.NoError(op.CreateFile(name, state, 0))
		_, err := op.SetFileMetadata(name, metadata.NewPersist(true))
		require.NoError(err)
	}

	clk.Add(tti + 1)

	require.NoError(m.deleteIdleOrExpiredFiles(op, tti, ttl, gauge))

	for _, name := range idle {
		_, err := op.GetFileStat(name)
		require.True(os.IsNotExist(err))
	}
	for _, name := range persisted {
		_, err := op.GetFileStat(name)
		require.NoError(err)
	}
}
