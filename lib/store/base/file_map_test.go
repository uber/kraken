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
package base

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/metadata"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func TestFileMapTryStore(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileMapLRUFixture()
	defer cleanup()

	fe := bundle.entry
	s1 := bundle.state1
	fm := bundle.fm

	require.False(fm.Contains(fe.GetName()))

	var wg sync.WaitGroup
	var successCount, skippedCount, errorCount uint32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			stored := fm.TryStore(fe.GetName(), fe, func(name string, entry FileEntry) bool {
				err = fe.Create(s1, 0)
				return err == nil
			})
			if err != nil {
				atomic.AddUint32(&errorCount, 1)
			} else if !stored {
				atomic.AddUint32(&skippedCount, 1)
			} else {
				atomic.AddUint32(&successCount, 1)
			}
		}()
	}
	wg.Wait()

	// Only one goroutine successfully stored the entry.
	require.Equal(errorCount, uint32(0))
	require.Equal(skippedCount, uint32(99))
	require.Equal(successCount, uint32(1))

	require.True(fm.Contains(fe.GetName()))
}

func TestFileMapTryStoreAborts(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileMapLRUFixture()
	defer cleanup()

	fe := bundle.entry
	s1 := bundle.state1
	fm := bundle.fm

	err := fe.Create(s1, 0)
	require.NoError(err)

	var wg sync.WaitGroup
	var successCount, skippedCount, errorCount uint32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			stored := fm.TryStore(fe.GetName(), fe, func(name string, entry FileEntry) bool {
				// Exit right away.
				err = os.ErrNotExist
				return false
			})
			if err != nil {
				atomic.AddUint32(&errorCount, 1)
			} else if !stored {
				atomic.AddUint32(&skippedCount, 1)
			} else {
				atomic.AddUint32(&successCount, 1)
			}
		}()
	}
	wg.Wait()

	// Some goroutines successfully stored the entry, executed f, encountered
	// failure and removed the entry.
	// Others might have loaded the temp entries and skipped.
	require.True(errorCount >= uint32(1))
	require.True(errorCount+skippedCount == uint32(100))
	require.Equal(successCount, uint32(0))
}

func TestFileMapLoadForRead(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileMapLRUFixture()
	defer cleanup()

	fe := bundle.entry
	s1 := bundle.state1
	fm := bundle.fm

	err := fe.Create(s1, 0)
	require.NoError(err)

	// Loading an non-existent entry does nothing.
	testInt := 1
	loaded := fm.LoadForWrite(fe.GetName(), func(name string, entry FileEntry) {
		testInt = 2
		return
	})
	require.False(loaded)
	require.Equal(testInt, 1)

	// Put entry into map.
	stored := fm.TryStore(fe.GetName(), fe, func(name string, entry FileEntry) bool {
		return true
	})
	require.True(stored)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loaded := fm.LoadForRead(fe.GetName(), func(name string, entry FileEntry) {
				_, err := fe.GetStat()
				require.NoError(err)
			})
			require.True(loaded)
		}()
	}
	wg.Wait()
}

func TestFileMapLoadForWrite(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileMapLRUFixture()
	defer cleanup()

	fe := bundle.entry
	s1 := bundle.state1
	s2 := bundle.state2
	fm := bundle.fm

	err := fe.Create(s1, 0)
	require.NoError(err)

	// Loading an non-existent entry does nothing.
	testInt := 1
	loaded := fm.LoadForWrite(fe.GetName(), func(name string, entry FileEntry) {
		testInt = 2
		return
	})
	require.False(loaded)
	require.Equal(testInt, 1)

	// Put entry into map.
	stored := fm.TryStore(fe.GetName(), fe, func(name string, entry FileEntry) bool {
		return true
	})
	require.True(stored)

	var wg sync.WaitGroup
	var successCount, stateErrorCount, otherErrorCount uint32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			loaded := fm.LoadForWrite(fe.GetName(), func(name string, entry FileEntry) {
				if fe.GetState() == s2 {
					atomic.AddUint32(&stateErrorCount, 1)
				} else {
					err = fe.Move(s2)
					if err == nil {
						atomic.AddUint32(&successCount, 1)
					} else {
						atomic.AddUint32(&otherErrorCount, 1)
					}
				}
			})
			require.True(loaded)
		}()
	}
	wg.Wait()

	// Only first goroutine successfully executed Move(), the others encountered
	// FileStateError.
	require.Equal(otherErrorCount, uint32(0))
	require.Equal(stateErrorCount, uint32(99))
	require.Equal(successCount, uint32(1))
}

func TestFileMapDelete(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileMapLRUFixture()
	defer cleanup()

	fe := bundle.entry
	s1 := bundle.state1
	fm := bundle.fm

	// Put entry into map.
	var err error
	stored := fm.TryStore(fe.GetName(), fe, func(name string, entry FileEntry) bool {
		err = fe.Create(s1, 0)
		return err == nil
	})
	require.True(stored)
	require.NoError(err)

	var wg sync.WaitGroup
	var successCount, skippedCount, errorCount uint32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error

			deleted := fm.Delete(fe.GetName(), func(name string, entry FileEntry) bool {
				err = fe.Delete()
				return err == nil
			})
			if err != nil {
				atomic.AddUint32(&errorCount, 1)
			} else if deleted {
				atomic.AddUint32(&successCount, 1)
			} else {
				atomic.AddUint32(&skippedCount, 1)
			}
		}()
	}
	wg.Wait()

	// Only the first goroutine successfully deleted the entry, the others skipped.
	require.Equal(errorCount, uint32(0))
	require.Equal(skippedCount, uint32(99))
	require.Equal(successCount, uint32(1))
}

func TestFileMapDeleteAbort(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileMapLRUFixture()
	defer cleanup()

	fe := bundle.entry
	s1 := bundle.state1
	fm := bundle.fm

	// Put entry into map.
	var err error
	stored := fm.TryStore(fe.GetName(), fe, func(name string, entry FileEntry) bool {
		err = fe.Create(s1, 0)
		return err == nil
	})
	require.True(stored)
	require.NoError(err)

	var wg sync.WaitGroup
	var successCount, skippedCount, errorCount uint32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error

			deleted := fm.Delete(fe.GetName(), func(name string, entry FileEntry) bool {
				err = os.ErrNotExist
				return true
			})
			if err != nil {
				atomic.AddUint32(&errorCount, 1)
			} else if deleted {
				atomic.AddUint32(&successCount, 1)
			} else {
				atomic.AddUint32(&skippedCount, 1)
			}
		}()
	}
	wg.Wait()

	// The first goroutine encountered error, but removed the entry from map
	// anyway. Other goroutines skipped.
	require.Equal(errorCount, uint32(1))
	require.Equal(skippedCount, uint32(99))
	require.Equal(successCount, uint32(0))
}

func TestLRUFileMapSizeLimit(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileStoreLRUFixture(100)
	defer cleanup()

	fm := bundle.store.fileMap
	state := bundle.state1

	insert := func(name string) {
		entry, err := NewLocalFileEntryFactory().Create(name, state)
		require.NoError(err)
		stored := fm.TryStore(name, entry, func(name string, entry FileEntry) bool {
			require.NoError(entry.Create(state, 0))
			return true
		})
		require.True(stored)
	}

	// Generate 101 file names.
	var names []string
	for i := 0; i < 101; i++ {
		names = append(names, fmt.Sprintf("test_file_%d", i))
	}

	// After inserting 100 files, the first file still exists in map.
	for _, name := range names[:100] {
		insert(name)
	}
	require.True(fm.Contains(names[0]))

	// Insert one more file entry beyond size limit.
	insert(names[100])

	// The first file should have been removed.
	require.False(fm.Contains(names[0]))
}

func TestLRUCreateLastAccessTimeOnCreateFile(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileStoreLRUFixture(100)
	defer cleanup()

	store := bundle.store
	clk := bundle.clk.(*clock.Mock)

	t0 := time.Now()
	clk.Set(t0)

	fn := "testfile123"
	s1 := bundle.state1

	require.NoError(store.NewFileOp().AcceptState(s1).CreateFile(fn, s1, 5))

	// Verify file exists.
	_, err := os.Stat(path.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(err)

	var lat metadata.LastAccessTime
	require.NoError(store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, &lat))
	require.Equal(t0.Truncate(time.Second), lat.Time)
}

func TestLRUUpdateLastAccessTimeOnMoveFrom(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileStoreLRUFixture(100)
	defer cleanup()

	store := bundle.store
	clk := bundle.clk.(*clock.Mock)

	t0 := time.Now()
	clk.Set(t0)

	s1, s2 := bundle.state1, bundle.state2

	name := core.DigestFixture().Hex()
	fp := filepath.Join(s1.GetDirectory(), name)
	f, err := os.Create(fp)
	require.NoError(err)
	f.Close()

	require.NoError(store.NewFileOp().AcceptState(s2).MoveFileFrom(name, s2, fp))

	var lat metadata.LastAccessTime
	require.NoError(store.NewFileOp().AcceptState(s2).GetFileMetadata(name, &lat))
	require.Equal(t0.Truncate(time.Second), lat.Time)
}

func TestLRUUpdateLastAccessTimeOnMove(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileStoreLRUFixture(100)
	defer cleanup()

	store := bundle.store
	clk := bundle.clk.(*clock.Mock)

	t0 := time.Now()
	clk.Set(t0)

	fn := "testfile123"
	s1, s2 := bundle.state1, bundle.state2

	require.NoError(store.NewFileOp().AcceptState(s1).CreateFile(fn, s1, 1))

	clk.Add(time.Hour)
	require.NoError(store.NewFileOp().AcceptState(s1).MoveFile(fn, s2))

	var lat metadata.LastAccessTime
	require.NoError(store.NewFileOp().AcceptState(s2).GetFileMetadata(fn, &lat))
	require.Equal(clk.Now().Truncate(time.Second), lat.Time)
}

func TestLRUUpdateLastAccessTimeOnOpen(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileStoreLRUFixture(100)
	defer cleanup()

	store := bundle.store
	clk := bundle.clk.(*clock.Mock)

	t0 := time.Now()
	clk.Set(t0)

	fn := "testfile123"
	s1 := bundle.state1

	require.NoError(store.NewFileOp().AcceptState(s1).CreateFile(fn, s1, 1))

	checkLAT := func(op FileOp, expected time.Time) {
		var lat metadata.LastAccessTime
		require.NoError(op.GetFileMetadata(fn, &lat))
		require.Equal(expected.Truncate(time.Second), lat.Time)
	}

	// No LAT change below resolution.
	clk.Add(time.Minute)
	_, err := store.NewFileOp().AcceptState(s1).GetFileReader(fn)
	require.NoError(err)
	checkLAT(store.NewFileOp().AcceptState(s1), t0)

	clk.Add(time.Hour)
	_, err = store.NewFileOp().AcceptState(s1).GetFileReader(fn)
	require.NoError(err)
	checkLAT(store.NewFileOp().AcceptState(s1), clk.Now())

	clk.Add(time.Hour)
	_, err = store.NewFileOp().AcceptState(s1).GetFileReadWriter(fn)
	require.NoError(err)
	checkLAT(store.NewFileOp().AcceptState(s1), clk.Now())
}

func TestLRUKeepLastAccessTimeOnPeek(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileStoreLRUFixture(100)
	defer cleanup()

	store := bundle.store
	clk := bundle.clk.(*clock.Mock)

	t0 := time.Now()
	clk.Set(t0)

	fn := "testfile123"
	s1 := bundle.state1

	require.NoError(store.NewFileOp().AcceptState(s1).CreateFile(fn, s1, 1))

	clk.Add(time.Hour)
	_, err := store.NewFileOp().AcceptState(s1).GetFileStat(fn)
	require.NoError(err)

	var lat metadata.LastAccessTime
	require.NoError(store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, &lat))
	require.Equal(t0.Truncate(time.Second), lat.Time)

	clk.Add(time.Hour)
	_, err = store.NewFileOp().AcceptState(s1).GetFilePath(fn)
	require.NoError(err)

	require.NoError(store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, &lat))
	require.Equal(t0.Truncate(time.Second), lat.Time)
}
