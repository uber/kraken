package internal

import (
	"math/rand"
	"path"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRCFileOp(t *testing.T) {
	tests := []func(require *require.Assertions, storeBundle *rcFileStoreTestBundle){
		testIncFileRefCount,
		testDecFileRefCount,
		testIncAndDecFileRefCountConcurrently,
	}

	for _, test := range tests {
		testName := runtime.FuncForPC(reflect.ValueOf(test).Pointer()).Name()
		t.Run(testName, func(t *testing.T) {
			require := require.New(t)
			store, cleanup := rcFileStoreFixture()
			defer cleanup()
			test(require, store)
		})
	}
}

func testIncFileRefCount(require *require.Assertions, storeBundle *rcFileStoreTestBundle) {
	store := storeBundle.store
	s1 := storeBundle.state1
	s2 := storeBundle.state2
	fn := storeBundle.files[s1]

	maxCount := rand.Intn(100) + 1
	var refCount int64
	var err error
	for j := 0; j < maxCount; j++ {
		// Inc
		refCount, err = store.NewFileOp().AcceptState(s1).(RCFileOp).IncFileRefCount(fn)
		require.NoError(err)
	}
	require.True(refCount == int64(maxCount))

	// Verify refcount.
	refCount, err = store.NewFileOp().AcceptState(s1).(RCFileOp).GetFileRefCount(fn)
	require.NoError(err)
	require.True(refCount >= int64(maxCount))

	// Try Delete.
	err = store.NewFileOp().AcceptState(s1).DeleteFile(fn)
	require.True(IsRefCountError(err))

	// Try MoveTo.
	err = store.NewFileOp().AcceptState(s1).MoveFileTo(fn, path.Join(s2.GetDirectory(), fn))
	require.True(IsRefCountError(err))
}

func testDecFileRefCount(require *require.Assertions, storeBundle *rcFileStoreTestBundle) {
	store := storeBundle.store
	s1 := storeBundle.state1
	fn := storeBundle.files[s1]

	// Increment then decrement ref count.
	maxCount := rand.Intn(100) + 1
	var refCount int64
	var err error
	for j := 0; j < maxCount; j++ {
		// Inc
		refCount, err = store.NewFileOp().AcceptState(s1).(RCFileOp).IncFileRefCount(fn)
		require.NoError(err)
	}
	require.True(refCount >= int64(maxCount))

	for j := 0; j < maxCount; j++ {
		// Dec
		refCount, err = store.NewFileOp().(RCFileOp).AcceptState(s1).(RCFileOp).DecFileRefCount(fn)
		require.NoError(err)
	}

	// Verify ref count.
	refCount, err = store.NewFileOp().AcceptState(s1).(RCFileOp).GetFileRefCount(fn)
	require.NoError(err)
	require.Equal(refCount, int64(0))

	// Try Delete. Should exceed this time.
	err = store.NewFileOp().AcceptState(s1).DeleteFile(fn)
	require.NoError(err)
}

func testIncAndDecFileRefCountConcurrently(require *require.Assertions, storeBundle *rcFileStoreTestBundle) {
	store := storeBundle.store
	s1 := storeBundle.state1
	fn := storeBundle.files[s1]

	// Increment and decrement ref count concurrently
	wg := &sync.WaitGroup{}
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			maxCount := rand.Intn(100) + 1
			var refCount int64
			var err error
			for j := 0; j < maxCount; j++ {
				// Inc
				refCount, err = store.NewFileOp().AcceptState(s1).(RCFileOp).IncFileRefCount(fn)
				require.NoError(err)
			}
			require.True(refCount >= int64(maxCount))

			// Verify ref count.
			refCount, err = store.NewFileOp().AcceptState(s1).(RCFileOp).GetFileRefCount(fn)
			require.NoError(err)
			require.True(refCount >= int64(maxCount))

			for j := 0; j < maxCount; j++ {
				// Dec
				refCount, err = store.NewFileOp().(RCFileOp).AcceptState(s1).(RCFileOp).DecFileRefCount(fn)
				require.NoError(err)
			}
		}()
	}
	wg.Wait()

	// Verify ref count.
	refCount, err := store.NewFileOp().AcceptState(s1).(RCFileOp).GetFileRefCount(fn)
	require.NoError(err)
	require.Equal(refCount, int64(0))
}
