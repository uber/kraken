package refcountable

import (
	"math/rand"
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/client/store/base"

	"github.com/stretchr/testify/assert"
)

func TestRefCount(t *testing.T) {
	// Setup
	cleanupTestRCFileEntry()
	backend, fe, err := getTestRCFileEntry()
	assert.Nil(t, err)
	defer cleanupTestRCFileEntry()

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
				refCount, err = fe.IncrementRefCount(dummyVerify)
				assert.Nil(t, err)
			}
			assert.True(t, refCount >= int64(maxCount))

			// Try Delete
			fileName, _ := fe.GetName(dummyVerify)
			err = backend.DeleteFile(fileName, []base.FileState{stateTest1})
			assert.True(t, IsRefCountError(err))

			for j := 0; j < maxCount; j++ {
				// Dec
				refCount, err = fe.DecrementRefCount(dummyVerify)
				assert.Nil(t, err)
			}
		}()
	}
	wg.Wait()

	refCount, err := fe.GetRefCount(dummyVerify)
	assert.Nil(t, err)
	assert.Equal(t, refCount, int64(0))
}
