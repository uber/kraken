package store

import (
	"os"
	"path"
	"sync"
	"testing"

	"io/ioutil"

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

func TestStoreBackend(t *testing.T) {
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
	_, err := backend.CreateFile(testFileName, []FileState{}, stateTest1, 5)
	assert.Nil(err)
	_, err = os.Stat(path.Join(stateTest1.GetDirectory(), testFileName))
	assert.False(os.IsNotExist(err))

	// Test getFileReadWriter
	var waitGroup sync.WaitGroup

	for i := 0; i < 100; i++ {
		waitGroup.Add(1)
		go func() {
			readWriter, err := backend.GetFileReadWriter(testFileName, []FileState{stateTest1})
			assert.Nil(err)

			_, err = readWriter.Write([]byte{'t', 'e', 's', 't', '\n'})
			assert.Nil(err)

			b := make([]byte, 3)
			_, err = readWriter.Seek(0, 0)
			assert.Nil(err)
			l, err := readWriter.Read(b)
			assert.Nil(err)
			assert.Equal(l, 3)
			assert.Equal(string(b[:l]), "tes")

			err = readWriter.Close()
			assert.Nil(err)
			waitGroup.Done()
		}()
	}
	waitGroup.Wait()

	// Test moveFile
	err = backend.MoveFile(testFileName, []FileState{stateTest1}, stateTest2)
	assert.Nil(err)
	// Test moveFile while it is open
	// preState: stateTest2
	// new state: stateTest1
	readWriterState2, err := backend.GetFileReadWriter(testFileName, []FileState{stateTest2})
	assert.Nil(err)
	err = backend.MoveFile(testFileName, []FileState{stateTest2}, stateTest1)
	assert.Nil(err)

	// Created hardlink in both stateTest1 and stateTest2
	_, err = os.Stat(path.Join(_testDir1, testFileName))
	assert.Nil(err)
	// the old file does not exist but still read/writable
	_, err = os.Stat(path.Join(_testDir2, testFileName))
	assert.True(os.IsNotExist(err))
	// Check goalstate
	f, _ := backend.(*localFileStoreBackend).fileMap.Load(testFileName)
	assert.Equal(f.(*localFileEntry).state, stateTest1)
	assert.Equal(f.(*localFileEntry).openCount, 1)
	// Create new readWriter at new state
	readWriterState1, err := backend.GetFileReadWriter(testFileName, []FileState{stateTest1})
	assert.Nil(err)
	f, _ = backend.(*localFileStoreBackend).fileMap.Load(testFileName)
	assert.Equal(f.(*localFileEntry).openCount, 2)
	// Check content
	dataState1, err := ioutil.ReadAll(readWriterState1)
	assert.Nil(err)
	dataState2, err := ioutil.ReadAll(readWriterState2)
	assert.Nil(err)
	assert.Equal(dataState1, dataState2)
	assert.Equal([]byte{'t', 'e', 's', 't', '\n'}, dataState1)
	// Write with old readWriter
	_, err = readWriterState1.WriteAt([]byte{'1'}, 0)
	assert.Nil(err)
	// Check content again
	readWriterState1.Seek(0, 0)
	readWriterState2.Seek(0, 0)
	dataState1, err = ioutil.ReadAll(readWriterState1)
	assert.Nil(err)
	dataState2, err = ioutil.ReadAll(readWriterState2)
	assert.Nil(err)
	assert.Equal(dataState1, dataState2)
	assert.Equal([]byte{'1', 'e', 's', 't', '\n'}, dataState1)
	// Close on last opened readwriter removes hardlink
	readWriterState2.Close()
	_, err = os.Stat(path.Join(_testDir2, testFileName))
	assert.True(os.IsNotExist(err))
	readWriterState1.Close()
	_, err = os.Stat(path.Join(_testDir1, testFileName))
	assert.Nil(err)
	// Check content again
	readWriterStateMoved, err := backend.GetFileReadWriter(testFileName, []FileState{stateTest1})
	assert.Nil(err)
	dataMoved, err := ioutil.ReadAll(readWriterStateMoved)
	assert.Nil(err)
	assert.Equal([]byte{'1', 'e', 's', 't', '\n'}, dataMoved)
	readWriterStateMoved.Close()

	err = backend.MoveFile(testFileName, []FileState{stateTest1}, stateTest2)
	assert.Nil(err)
	f, _ = backend.(*localFileStoreBackend).fileMap.Load(testFileName)
	assert.Equal(f.(*localFileEntry).state, stateTest2)
	assert.Equal(f.(*localFileEntry).openCount, 0)

	// Test getFileReader
	for i := 0; i < 100; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			reader, err := backend.GetFileReader(testFileName, []FileState{stateTest2})
			assert.Nil(err)

			b := make([]byte, 5)
			_, err = reader.Seek(0, 0)
			assert.Nil(err)
			l, err := reader.ReadAt(b, 0)
			assert.Nil(err)
			assert.Equal(l, 5)
			assert.Equal(string(b[:l]), "1est\n")

			err = reader.Close()
			assert.Nil(err)
		}()
	}
	waitGroup.Wait()

	// Confirm openCount is 0.
	reader, err := backend.GetFileReader(testFileName, []FileState{stateTest2})
	reader.Close()
	assert.Equal(reader.(*localFileReadWriter).entry.openCount, 0)
	assert.False(reader.(*localFileReadWriter).entry.IsOpen(nil))

	// Test deleting file.
	err = backend.DeleteFile(testFileName, []FileState{stateTest2})
	assert.Equal(err, nil)
	_, err = os.Stat(path.Join(_testDir1, testFileName))
	assert.True(os.IsNotExist(err))
}
