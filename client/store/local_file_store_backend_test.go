package store

import (
	"fmt"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockFileState int

const (
	stateTest1 mockFileState = iota
	stateTest2
	stateTest3
)

var _testRoot = "./.tmp/test/"
var _testDir1 = "./.tmp/test/test1"
var _testDir2 = "./.tmp/test/test2"
var _testDir3 = "./.tmp/test/test3"

var _mockFileStateLookup = [...]string{_testDir1, _testDir2, _testDir3}

func (state mockFileState) GetDirectory() string { return _mockFileStateLookup[state] }

func TestStore(t *testing.T) {
	// Setup
	assert := require.New(t)

	var testFileName = "test_file.txt"
	os.RemoveAll(_testRoot)
	if _, err := os.Stat(_testRoot); os.IsNotExist(err) {
		os.MkdirAll(_testRoot, 0777)
	}
	if _, err := os.Stat(_testDir1); os.IsNotExist(err) {
		os.MkdirAll(_testDir1, 0777)
	}
	if _, err := os.Stat(_testDir2); os.IsNotExist(err) {
		os.MkdirAll(_testDir2, 0777)
	}
	if _, err := os.Stat(_testDir3); os.IsNotExist(err) {
		os.MkdirAll(_testDir3, 0777)
	}
	defer os.RemoveAll(_testRoot)

	// Test createEmptyFile
	backend := NewLocalFileStoreBackend()
	err := backend.CreateEmptyFile(testFileName, stateTest1, 5)
	assert.Nil(err)
	_, err = os.Stat(path.Join(stateTest1.GetDirectory(), testFileName))
	assert.False(os.IsNotExist(err))

	// Test getFileReadWriter
	var waitGroup sync.WaitGroup

	fmt.Println("test0")

	for i := 0; i < 100; i++ {
		waitGroup.Add(1)
		go func() {

			readWriter, err := backend.GetFileReadWriter(testFileName, stateTest1)
			assert.Nil(err)

			_, err = readWriter.Write([]byte{'t', 'e', 's', 't', '\n'})
			assert.Nil(err)

			b := make([]byte, 3)
			_, err = readWriter.Seek(0, 0)
			assert.Nil(err)
			l, err := readWriter.Read(b)
			assert.Nil(err)
			fmt.Println(l)
			assert.Equal(l, 3)
			assert.Equal(string(b[:l]), "tes")

			err = readWriter.Close()
			assert.Nil(err)
			waitGroup.Done()
		}()
	}
	waitGroup.Wait()

	// Test moveFile
	err = backend.MoveFile(testFileName, stateTest1, stateTest2)
	assert.Nil(err)

	// Test getFileReader
	for i := 0; i < 100; i++ {
		waitGroup.Add(1)
		go func() {
			reader, err := backend.GetFileReader(testFileName, stateTest2)
			assert.Nil(err)

			b := make([]byte, 5)
			_, err = reader.Seek(0, 0)
			assert.Nil(err)
			l, err := reader.ReadAt(b, 0)
			assert.Nil(err)
			assert.Equal(l, 5)
			assert.Equal(string(b[:l]), "test\n")

			err = reader.Close()
			assert.Nil(err)
			waitGroup.Done()
		}()
	}
	waitGroup.Wait()

	// Confirm openCount is 0.
	reader, err := backend.GetFileReader(testFileName, stateTest2)
	reader.Close()
	assert.Equal(reader.(localFileReadWriter).entry.openCount, 0)
	assert.False(reader.(localFileReadWriter).entry.IsOpen())

	// Test deleting file.
	err = backend.DeleteFile(testFileName, stateTest2)
	assert.Equal(err, nil)
	_, err = os.Stat(path.Join(_testDir1, testFileName))
	assert.True(os.IsNotExist(err))
}
