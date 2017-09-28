package base

import (
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateEmptyFile(t *testing.T) {
	// Setup
	s, cleanup := getTestFileStore()
	defer cleanup()

	// Create empty file
	err := s.CreateFile(testFileName, []FileState{}, stateTest1, 5)
	assert.Nil(t, err)
	_, err = os.Stat(path.Join(stateTest1.GetDirectory(), testFileName[0:2], testFileName[2:4], testFileName))
	assert.False(t, os.IsNotExist(err))
}

func TestGetFileReadWriter(t *testing.T) {
	// Setup
	s, cleanup := getTestFileStore()
	defer cleanup()

	err := s.CreateFile(testFileName, []FileState{}, stateTest1, 5)
	assert.Nil(t, err)

	// Get ReadWriter and modify file concurrently
	var waitGroup sync.WaitGroup
	for i := 0; i < 100; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			readWriter, err := s.GetFileReadWriter(testFileName, []FileState{stateTest1})
			assert.Nil(t, err)

			_, err = readWriter.Write([]byte{'t', 'e', 's', 't', '\n'})
			assert.Nil(t, err)

			b := make([]byte, 3)
			_, err = readWriter.Seek(0, 0)
			assert.Nil(t, err)
			l, err := readWriter.Read(b)
			assert.Nil(t, err)
			assert.Equal(t, l, 3)
			assert.Equal(t, string(b[:l]), "tes")

			err = readWriter.Close()
			assert.Nil(t, err)

			// Verify size() still works after readwriter is closed.
			size := readWriter.Size()
			assert.Equal(t, size, int64(5))
		}()
	}
	waitGroup.Wait()

	// Test getFileReader
	for i := 0; i < 100; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			reader, err := s.GetFileReader(testFileName, []FileState{stateTest1})
			assert.Nil(t, err)

			b := make([]byte, 5)
			_, err = reader.Seek(0, 0)
			assert.Nil(t, err)
			l, err := reader.ReadAt(b, 0)
			assert.Nil(t, err)
			assert.Equal(t, l, 5)
			assert.Equal(t, string(b[:l]), "test\n")

			err = reader.Close()
			assert.Nil(t, err)
		}()
	}
	waitGroup.Wait()

	reader, err := s.GetFileReader(testFileName, []FileState{stateTest1})
	reader.Close()
}

func TestMoveFile(t *testing.T) {
	// Setup
	s, cleanup := getTestFileStore()
	defer cleanup()

	err := s.CreateFile(testFileName, []FileState{}, stateTest1, 5)
	assert.Nil(t, err)

	// Move file to stateTest2
	err = s.MoveFile(testFileName, []FileState{stateTest1}, stateTest2)
	assert.Nil(t, err)

	// Update content
	readWriterState2, err := s.GetFileReadWriter(testFileName, []FileState{stateTest2})
	assert.Nil(t, err)
	_, err = readWriterState2.Write([]byte{'t', 'e', 's', 't', '\n'})
	assert.Nil(t, err)
	readWriterState2.Close()
	readWriterState2, err = s.GetFileReadWriter(testFileName, []FileState{stateTest2})
	assert.Nil(t, err)

	// Move back to stateTest1
	err = s.MoveFile(testFileName, []FileState{stateTest2}, stateTest1)
	assert.Nil(t, err)

	// Created hardlink in both stateTest1
	_, err = os.Stat(path.Join(testDir1, testFileName[0:2], testFileName[2:4], testFileName))
	assert.Nil(t, err)
	// the old file does not exist but still read/writable
	_, err = os.Stat(path.Join(testDir2, testFileName[0:2], testFileName[2:4], testFileName))
	assert.True(t, os.IsNotExist(err))
	// Check state
	f, _, _ := s.LoadFileEntry(testFileName, []FileState{stateTest1})
	assert.Equal(t, f.(*LocalFileEntry).state, stateTest1)
	// Create new readWriter at new state
	readWriterState1, err := s.GetFileReadWriter(testFileName, []FileState{stateTest1})
	assert.Nil(t, err)
	// Check content
	dataState1, err := ioutil.ReadAll(readWriterState1)
	assert.Nil(t, err)
	dataState2, err := ioutil.ReadAll(readWriterState2)
	assert.Nil(t, err)
	assert.Equal(t, dataState1, dataState2)
	assert.Equal(t, []byte{'t', 'e', 's', 't', '\n'}, dataState1)
	// Write with old readWriter
	_, err = readWriterState1.WriteAt([]byte{'1'}, 0)
	assert.Nil(t, err)
	// Check content again
	readWriterState1.Seek(0, 0)
	readWriterState2.Seek(0, 0)
	dataState1, err = ioutil.ReadAll(readWriterState1)
	assert.Nil(t, err)
	dataState2, err = ioutil.ReadAll(readWriterState2)
	assert.Nil(t, err)
	assert.Equal(t, dataState1, dataState2)
	assert.Equal(t, []byte{'1', 'e', 's', 't', '\n'}, dataState1)
	// Close on last opened readwriter removes hardlink
	readWriterState2.Close()
	_, err = os.Stat(path.Join(testDir2, testFileName[0:2], testFileName[2:4], testFileName))
	assert.True(t, os.IsNotExist(err))
	readWriterState1.Close()
	_, err = os.Stat(path.Join(testDir1, testFileName[0:2], testFileName[2:4], testFileName))
	assert.Nil(t, err)
	// Check content again
	readWriterStateMoved, err := s.GetFileReadWriter(testFileName, []FileState{stateTest1})
	assert.Nil(t, err)
	dataMoved, err := ioutil.ReadAll(readWriterStateMoved)
	assert.Nil(t, err)
	assert.Equal(t, []byte{'1', 'e', 's', 't', '\n'}, dataMoved)
	readWriterStateMoved.Close()

	// Move to stateTest2 again
	err = s.MoveFile(testFileName, []FileState{stateTest1}, stateTest2)
	assert.Nil(t, err)
	f, _, _ = s.LoadFileEntry(testFileName, []FileState{stateTest2})
	assert.Equal(t, f.(*LocalFileEntry).state, stateTest2)
}

func TestDeleteFile(t *testing.T) {
	// Setup
	s, cleanup := getTestFileStore()
	defer cleanup()

	err := s.CreateFile(testFileName, []FileState{}, stateTest1, 5)
	assert.Nil(t, err)

	// Test deleting file.
	err = s.DeleteFile(testFileName, []FileState{stateTest1})
	assert.Equal(t, err, nil)
	_, err = os.Stat(path.Join(testDir1, testFileName[0:2], testFileName[2:4], testFileName))
	assert.True(t, os.IsNotExist(err))
}
