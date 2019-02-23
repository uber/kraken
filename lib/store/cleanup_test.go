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
package store

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/base"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/testutil"

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

	state := base.NewFileState(dir)

	store := base.NewLocalFileStore(clk)

	return state, store.NewFileOp().AcceptState(state), cleanup.Run
}

func TestCleanupManagerAddJob(t *testing.T) {
	require := require.New(t)

	clk := clock.New()

	m, err := newCleanupManager(clk, tally.NoopScope)
	require.NoError(err)
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

	_, err = op.GetFileStat(name)
	require.True(os.IsNotExist(err))
}

func TestCleanupManagerDeleteIdleFiles(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	clk.Set(time.Now())
	tti := 6 * time.Hour
	ttl := 24 * time.Hour

	m, err := newCleanupManager(clk, tally.NoopScope)
	require.NoError(err)
	defer m.stop()

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

	_, err = m.scan(op, tti, ttl)
	require.NoError(err)

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

	m, err := newCleanupManager(clk, tally.NoopScope)
	require.NoError(err)
	defer m.stop()

	state, op, cleanup := fileOpFixture(clk)
	defer cleanup()

	var names []string
	for i := 0; i < 10; i++ {
		names = append(names, core.DigestFixture().Hex())
	}
	for _, name := range names {
		require.NoError(op.CreateFile(name, state, 0))
	}

	_, err = m.scan(op, tti, ttl)
	require.NoError(err)

	for _, name := range names {
		_, err := op.GetFileStat(name)
		require.NoError(err)
	}

	clk.Add(ttl + 1)

	_, err = m.scan(op, tti, ttl)
	require.NoError(err)

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

	m, err := newCleanupManager(clk, tally.NoopScope)
	require.NoError(err)
	defer m.stop()

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

	_, err = m.scan(op, tti, ttl)
	require.NoError(err)

	for _, name := range idle {
		_, err := op.GetFileStat(name)
		require.True(os.IsNotExist(err))
	}
	for _, name := range persisted {
		_, err := op.GetFileStat(name)
		require.NoError(err)
	}
}

func TestCleanupManageDiskUsage(t *testing.T) {
	require := require.New(t)

	clk := clock.New()

	m, err := newCleanupManager(clk, tally.NoopScope)
	require.NoError(err)
	defer m.stop()

	state, op, cleanup := fileOpFixture(clk)
	defer cleanup()

	for i := 0; i < 100; i++ {
		require.NoError(op.CreateFile(core.DigestFixture().Hex(), state, 5))
	}

	usage, err := m.scan(op, time.Hour, time.Hour)
	require.NoError(err)
	require.Equal(int64(500), usage)
}
