package lockermap

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapTryStoreReturnsFalseOnDuplicates(t *testing.T) {
	require := require.New(t)
	var m Map

	require.True(m.TryStore("k", new(sync.Mutex)))
	require.False(m.TryStore("k", new(sync.Mutex)))
}

type testValue struct {
	sync.Mutex
	n int
}

func TestMapLoadHoldsLock(t *testing.T) {
	require := require.New(t)
	var m Map

	require.True(m.TryStore("k", new(testValue)))

	// Only a single goroutine should be able to increment n.
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go require.True(m.Load("k", func(l sync.Locker) {
			defer wg.Done()
			v := l.(*testValue)
			if v.n == 0 {
				v.n++
			} else {
				require.Equal(1, v.n)
			}
		}))
	}
	wg.Wait()
}

type testValueCoordinatedLock struct {
	mu      sync.Mutex
	loading bool
	locking chan bool
	deleted chan bool
}

func (v *testValueCoordinatedLock) Lock() {
	if v.loading {
		v.locking <- true
		<-v.deleted
	}
	v.mu.Lock()
}

func (v *testValueCoordinatedLock) Unlock() { v.mu.Unlock() }

func newTestValueCoordinatedLock() *testValueCoordinatedLock {
	return &testValueCoordinatedLock{
		locking: make(chan bool),
		deleted: make(chan bool),
	}
}

func TestMapLoadReturnsFalseWhenKeyDeletedBeforeValueLocked(t *testing.T) {
	require := require.New(t)
	var m Map

	v := newTestValueCoordinatedLock()

	require.True(m.TryStore("k", v))

	var wg sync.WaitGroup
	wg.Add(1)

	// The goroutine should be able to load k, but k is deleted before it can
	// acquire the value lock.
	v.loading = true
	go func() {
		defer wg.Done()
		require.False(m.Load("k", func(l sync.Locker) {}))
	}()

	<-v.locking
	v.loading = false
	m.Delete("k")
	v.deleted <- true

	wg.Wait()
}

func TestMapRangeSkipsWhenKeyDeletedBeforeValueLocked(t *testing.T) {
	require := require.New(t)
	var m Map

	v := newTestValueCoordinatedLock()

	require.True(m.TryStore("k", v))

	var wg sync.WaitGroup
	wg.Add(1)

	// The goroutine should be able to load k, but k is deleted before it can
	// acquire the value lock, and thus range should never execute on anything.
	v.loading = true
	go func() {
		defer wg.Done()
		m.Range(func(k interface{}, v sync.Locker) bool {
			require.Fail("Should not be able to execute Range iteration")
			return true
		})
	}()

	<-v.locking
	v.loading = false
	m.Delete("k")
	v.deleted <- true

	wg.Wait()
}
