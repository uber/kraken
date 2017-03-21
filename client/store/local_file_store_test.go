package store

import (
	"io"
	"os"
	"path"
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/configuration"

	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	// Setup
	assert := require.New(t)

	var testFileName = "local_file_test_input.txt"
	var testRoot = "./.tmp/test/"
	var sourceRoot = path.Join(testRoot, "source")
	var storeRoot = path.Join(testRoot, "store")
	var trashRoot = path.Join(testRoot, "trash")
	var testConfig = &configuration.Config{
		DownloadDir: sourceRoot,
		CacheDir:    storeRoot,
		TrashDir:    trashRoot,
	}
	os.RemoveAll(testRoot)
	if _, err := os.Stat(sourceRoot); os.IsNotExist(err) {
		os.MkdirAll(sourceRoot, 0777)
	}
	if _, err := os.Stat(storeRoot); os.IsNotExist(err) {
		os.MkdirAll(storeRoot, 0777)
	}
	if _, err := os.Stat(trashRoot); os.IsNotExist(err) {
		os.MkdirAll(trashRoot, 0777)
	}
	defer os.RemoveAll(testRoot)
	if _, err := os.Stat(path.Join(sourceRoot, testFileName)); os.IsNotExist(err) {
		testFile, _ := os.Open(path.Join("./", testFileName))
		sourceFile, _ := os.Create(path.Join(sourceRoot, testFileName))
		_, err := io.Copy(sourceFile, testFile)
		assert.Equal(err, nil)
		testFile.Close()
		sourceFile.Close()
	}

	// Test Add
	store := NewLocalFileStore(testConfig)
	err := store.Add(testFileName)
	assert.Equal(err, nil)
	_, err = os.Stat(path.Join(sourceRoot, testFileName))
	assert.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(storeRoot, testFileName))
	assert.False(os.IsNotExist(err))

	// Test Get and Close
	var waitGroup sync.WaitGroup
	for i := 0; i < 100; i++ {
		waitGroup.Add(1)
		go func() {
			err = store.Add(testFileName)
			assert.NotEqual(err, nil)

			reader, err := store.Get(testFileName)
			assert.Equal(err, nil)

			b := make([]byte, 20)
			l, _ := reader.descriptor.Read(b)
			assert.Equal(l, 5)
			assert.Equal(string(b[:l]), "test\n", "Same")

			reader.Close()

			waitGroup.Done()
		}()
	}
	waitGroup.Wait()

	// Confirm openCount is 0.
	reader, err := store.Get(testFileName)
	assert.Equal(err, nil)
	reader.Close()
	assert.Equal(reader.localFile.openCount, 0)
	assert.False(reader.localFile.isOpen())

	// Test deleting file.
	err = store.Delete(testFileName)
	assert.Equal(err, nil)
	_, err = os.Stat(path.Join(storeRoot, testFileName))
	assert.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(trashRoot, testFileName))
	assert.False(os.IsNotExist(err))
}
