package base

import (
	"fmt"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// Tests for LRUFileMap
func TestFileMapLRU(t *testing.T) {
	tests := []func(require *require.Assertions, bundle *fileMapTestBundle){
		testLRUFileMapSizeLimit,
	}

	for _, test := range tests {
		testName := runtime.FuncForPC(reflect.ValueOf(test).Pointer()).Name()
		t.Run(testName, func(t *testing.T) {
			require := require.New(t)
			fm, cleanup := fileMapLRUFixture()
			defer cleanup()
			test(require, fm)
		})
	}
}

func testLRUFileMapSizeLimit(require *require.Assertions, bundle *fileMapTestBundle) {
	s1 := bundle.state1
	fm := bundle.fm

	var successCount, skippedCount, errCount uint32
	for i := 0; i < 100; i++ {
		var err error
		name := fmt.Sprintf("test_file_%d", i)
		entry := NewLocalFileEntryFactory().Create(name, s1)
		_, loaded := fm.LoadOrStore(name, entry, func(name string, entry FileEntry) error {
			err = entry.Create(s1, 0)
			return err
		})
		if loaded {
			atomic.AddUint32(&skippedCount, 1)
		} else if err != nil {
			atomic.AddUint32(&errCount, 1)
		} else {
			atomic.AddUint32(&successCount, 1)
		}
	}

	// All should have succeeded.
	require.Equal(errCount, uint32(0))
	require.Equal(skippedCount, uint32(0))
	require.Equal(successCount, uint32(100))

	// The first file exists in map.
	require.True(fm.Contains("test_file_0"))

	// Insert one more file entry beyond size limit.
	var err error
	name := fmt.Sprintf("test_file_%d", 100)
	entry := NewLocalFileEntryFactory().Create(name, s1)
	_, loaded := fm.LoadOrStore(name, entry, func(name string, entry FileEntry) error {
		err = entry.Create(s1, 0)
		return err
	})
	require.NoError(err)
	require.False(loaded)

	// The first file should have been removed.
	require.False(fm.Contains("test_file_0"))
}
