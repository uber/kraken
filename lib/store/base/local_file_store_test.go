package base

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests should pass for all FileStore implementations
func TestLocalFileStore(t *testing.T) {
	stores := []struct {
		name    string
		fixture func() (storeBundle *fileStoreTestBundle, cleanup func())
	}{
		{"Test LocalFileStoreDefault", fileStoreDefaultFixture},
		{"Test LocalFileStoreSharded", fileStoreShardDefaultFixture},
		{"Test LocalFileStoreLRU", func() (storeBundle *fileStoreTestBundle, cleanup func()) { return fileStoreLRUFixture(2) }},
	}

	tests := []struct {
		name string
		f    func(require *require.Assertions, storeBundle *fileStoreTestBundle)
	}{
		{"Test CreateFile", testCreateFile},
		{"Test LoadFileEntry", testLoadFileEntry},
		{"Test GetFileReaderWriter", testGetFileReadWriter},
		{"Test MoveFile", testMoveFile},
		{"Test DeleteFile", testDeleteFile},
	}

	for _, store := range stores {
		t.Run(store.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					require := require.New(t)
					s, cleanup := store.fixture()
					defer cleanup()
					test.f(require, s)
				})
			}
		})
	}
}

func testCreateFile(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	fn := "testfile123"
	// Create empty file
	err := store.CreateFile(fn, []FileState{}, storeBundle.state1, 5)
	require.NoError(err)
	_, err = os.Stat(path.Join(storeBundle.state1.GetDirectory(), store.fileEntryInternalFactory.GetRelativePath(fn)))
	require.False(os.IsNotExist(err))
}

func testLoadFileEntry(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	fn := "testfileondisk"
	require.NoError(store.CreateFile(fn, []FileState{}, storeBundle.state1, 5))
	_, err := os.Stat(path.Join(storeBundle.state1.GetDirectory(), store.fileEntryInternalFactory.GetRelativePath(fn)))
	require.NoError(err)
	_, ok := store.fileMap.Load(fn)
	require.True(ok)

	// Recreate store nukes store's in memory map
	storeBundle.recreateStore()
	store = storeBundle.store
	_, ok = store.fileMap.Load(fn)
	require.False(ok)
	// GetFileReader should load file from disk into map
	_, err = store.GetFileReader(fn, []FileState{storeBundle.state1})
	require.NoError(err)
	_, ok = store.fileMap.Load(fn)
	require.True(ok)
}

func testGetFileReadWriter(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	fileBundle, ok := storeBundle.files[storeBundle.state1]
	if !ok {
		log.Fatal("file not found in state1")
	}
	fn := fileBundle.name

	// Get ReadWriter and modify file concurrently
	var waitGroup sync.WaitGroup
	for i := 0; i < 100; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			readWriter, err := store.GetFileReadWriter(fn, []FileState{storeBundle.state1})
			require.NoError(err)

			_, err = readWriter.Write([]byte{'t', 'e', 's', 't', '\n'})
			require.NoError(err)

			b := make([]byte, 3)
			_, err = readWriter.Seek(0, 0)
			require.NoError(err)
			l, err := readWriter.Read(b)
			require.NoError(err)
			require.Equal(l, 3)
			require.Equal(string(b[:l]), "tes")

			err = readWriter.Close()
			require.NoError(err)

			// Verify size() still works after readwriter is closed.
			size := readWriter.Size()
			require.Equal(size, int64(5))
		}()
	}
	waitGroup.Wait()

	// Test getFileReader
	for i := 0; i < 100; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			reader, err := store.GetFileReader(fn, []FileState{storeBundle.state1})
			require.NoError(err)

			b := make([]byte, 5)
			_, err = reader.Seek(0, 0)
			require.NoError(err)
			l, err := reader.ReadAt(b, 0)
			require.NoError(err)
			require.Equal(l, 5)
			require.Equal(string(b[:l]), "test\n")

			err = reader.Close()
			require.NoError(err)
		}()
	}
	waitGroup.Wait()

	reader, err := store.GetFileReader(fn, []FileState{storeBundle.state1})
	require.NoError(err)
	reader.Close()
}

func testMoveFile(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	fileBundle, ok := storeBundle.files[storeBundle.state1]
	if !ok {
		log.Fatal("file not found in state1")
	}
	fn := fileBundle.name

	// Move file to stateTest2
	err := store.MoveFile(fn, []FileState{storeBundle.state1}, storeBundle.state2)
	require.NoError(err)

	// Update content
	readWriterState2, err := store.GetFileReadWriter(fn, []FileState{storeBundle.state2})
	require.NoError(err)
	_, err = readWriterState2.Write([]byte{'t', 'e', 's', 't', '\n'})
	require.NoError(err)
	readWriterState2.Close()
	readWriterState2, err = store.GetFileReadWriter(fn, []FileState{storeBundle.state2})
	require.NoError(err)

	// Move back to stateTest1
	err = store.MoveFile(fn, []FileState{storeBundle.state2}, storeBundle.state1)
	require.NoError(err)

	// Created hardlink in both stateTest1
	_, err = os.Stat(path.Join(storeBundle.state1.dir, store.fileEntryInternalFactory.GetRelativePath(fn)))
	require.NoError(err)
	// the old file does not exist but still read/writable
	_, err = os.Stat(path.Join(storeBundle.state2.dir, store.fileEntryInternalFactory.GetRelativePath(fn)))
	require.True(os.IsNotExist(err))
	// Check state
	f, _, _ := store.LoadFileEntry(fn, []FileState{storeBundle.state1})
	require.Equal(f.(*LocalFileEntry).state, storeBundle.state1)
	// Create new readWriter at new state
	readWriterState1, err := store.GetFileReadWriter(fn, []FileState{storeBundle.state1})
	require.NoError(err)
	// Check content
	dataState1, err := ioutil.ReadAll(readWriterState1)
	require.NoError(err)
	dataState2, err := ioutil.ReadAll(readWriterState2)
	require.NoError(err)
	require.Equal(dataState1, dataState2)
	require.Equal([]byte{'t', 'e', 's', 't', '\n'}, dataState1)
	// Write with old readWriter
	_, err = readWriterState1.WriteAt([]byte{'1'}, 0)
	require.NoError(err)
	// Check content again
	readWriterState1.Seek(0, 0)
	readWriterState2.Seek(0, 0)
	dataState1, err = ioutil.ReadAll(readWriterState1)
	require.NoError(err)
	dataState2, err = ioutil.ReadAll(readWriterState2)
	require.NoError(err)
	require.Equal(dataState1, dataState2)
	require.Equal([]byte{'1', 'e', 's', 't', '\n'}, dataState1)
	// Close on last opened readwriter removes hardlink
	readWriterState2.Close()
	_, err = os.Stat(path.Join(storeBundle.state2.dir, store.fileEntryInternalFactory.GetRelativePath(fn)))
	require.True(os.IsNotExist(err))
	readWriterState1.Close()
	_, err = os.Stat(path.Join(storeBundle.state1.dir, store.fileEntryInternalFactory.GetRelativePath(fn)))
	require.NoError(err)
	// Check content again
	readWriterStateMoved, err := store.GetFileReadWriter(fn, []FileState{storeBundle.state1})
	require.NoError(err)
	dataMoved, err := ioutil.ReadAll(readWriterStateMoved)
	require.NoError(err)
	require.Equal([]byte{'1', 'e', 's', 't', '\n'}, dataMoved)
	readWriterStateMoved.Close()

	// Move to stateTest2 again
	err = store.MoveFile(fn, []FileState{storeBundle.state1}, storeBundle.state2)
	require.NoError(err)
	f, _, _ = store.LoadFileEntry(fn, []FileState{storeBundle.state2})
	require.Equal(f.(*LocalFileEntry).state, storeBundle.state2)
}

func testDeleteFile(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store
	fileBundle, ok := storeBundle.files[storeBundle.state1]
	if !ok {
		log.Fatal("file not found in state1")
	}
	fn := fileBundle.name
	content := "this a test for read after delete"

	// Write to file
	rw, err := store.GetFileReadWriter(fn, []FileState{storeBundle.state1})
	require.NoError(err)
	rw.Write([]byte(content))

	// Confirm deletion
	err = store.DeleteFile(fn, []FileState{storeBundle.state1})
	require.NoError(err)
	_, err = os.Stat(path.Join(storeBundle.state1.dir, store.fileEntryInternalFactory.GetRelativePath(fn)))
	require.True(os.IsNotExist(err))

	// Existing readwriter should still work after deletion
	rw.Seek(0, 0)
	data, err := ioutil.ReadAll(rw)
	require.NoError(err)
	require.Equal(content, string(data))

	rw.Write([]byte(content))
	rw.Seek(0, 0)
	data, err = ioutil.ReadAll(rw)
	require.NoError(err)
	require.Equal(content+content, string(data))

	rw.Close()

	// Get deleted file should fail
	_, err = store.GetFileReader(fn, []FileState{storeBundle.state1})
	require.True(os.IsNotExist(err))
}
