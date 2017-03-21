package store

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenCount(t *testing.T) {
	assert := require.New(t)

	var testFileName = "local_file_test_input.txt"
	localFile := NewLocalFile("./"+testFileName, testFileName)

	var waitGroup sync.WaitGroup
	for i := 0; i < 100; i++ {
		waitGroup.Add(1)
		go func() {
			reader, _ := NewLocalFileReader(localFile)
			assert.True(localFile.isOpen())

			b := make([]byte, 20)
			l, _ := reader.Read(b)
			assert.Equal(l, 5)
			assert.Equal(string(b[:l]), "test\n", "Same")

			reader.Close()

			// Close again, which just returns error.
			err := reader.Close()
			assert.NotEqual(err, nil)

			waitGroup.Done()
		}()
	}

	waitGroup.Wait()
	assert.Equal(localFile.openCount, 0)
	assert.False(localFile.isOpen())
}
