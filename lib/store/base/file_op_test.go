// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package base

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/stretchr/testify/require"
)

// These tests should pass for all FileStore/FileOp implementations
func TestFileOp(t *testing.T) {
	stores := []struct {
		name    string
		fixture func() (storeBundle *fileStoreTestBundle, cleanup func())
	}{
		{"LocalFileStoreDefault", fileStoreDefaultFixture},
		{"LocalFileStoreCAS", fileStoreCASFixture},
		{"LocalFileStoreLRU", func() (storeBundle *fileStoreTestBundle, cleanup func()) {
			return fileStoreLRUFixture(2)
		}},
	}

	tests := []func(require *require.Assertions, storeBundle *fileStoreTestBundle){
		testCreateFile,
		testCreateFileFail,
		testReloadFileEntry,
		testMoveFile,
		testLinkFileTo,
		testDeleteFile,
		testGetFileReader,
		testGetFileReadWriter,
		testGetOrSetFileMetadataConcurrently,
		testSetFileMetadataAtConcurrently,
		testDeleteFileMetadata,
	}

	for _, store := range stores {
		t.Run(store.name, func(t *testing.T) {
			for _, test := range tests {
				testName := runtime.FuncForPC(reflect.ValueOf(test).Pointer()).Name()
				t.Run(testName, func(t *testing.T) {
					require := require.New(t)
					s, cleanup := store.fixture()
					defer cleanup()
					test(require, s)
				})
			}
		})
	}
}

func testCreateFile(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	fn := core.DigestFixture().Hex()
	s1 := storeBundle.state1
	s2 := storeBundle.state2

	var wg sync.WaitGroup
	var successCount, existsErrorCount uint32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Create empty file.
			if err := store.NewFileOp().AcceptState(s1).CreateFile(fn, s1, 5); err == nil {
				atomic.AddUint32(&successCount, 1)
			} else if os.IsExist(err) {
				atomic.AddUint32(&existsErrorCount, 1)
			}
		}()
	}
	wg.Wait()

	require.Equal(successCount, uint32(1))
	require.Equal(existsErrorCount, uint32(99))

	// Verify file exists.
	_, err := os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(err)

	// Create file again with different target state, but include state of existing file as an acceptable state.
	err = store.NewFileOp().AcceptState(s1).CreateFile(fn, s2, 5)
	require.Error(err)
	require.True(os.IsExist(err))
	_, err = os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(err)
}

func testCreateFileFail(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	fn := core.DigestFixture().Hex()
	s1 := storeBundle.state1
	s2 := storeBundle.state2
	s3 := storeBundle.state3

	// Create empty file
	err := store.NewFileOp().AcceptState(s1).CreateFile(fn, s1, 5)
	require.NoError(err)
	_, err = os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(err)

	// Create file again with different target state
	err = store.NewFileOp().AcceptState(s3).CreateFile(fn, s2, 5)
	require.Error(err)
	require.True(IsFileStateError(err))
	require.True(strings.HasPrefix(err.Error(), "failed to perform"))
	_, err = os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(err)
}

func testReloadFileEntry(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	fn := core.DigestFixture().Hex()
	m := getMockMetadataOne()
	m.content = []byte("foo")
	s1 := storeBundle.state1

	// Create file
	require.NoError(store.NewFileOp().CreateFile(fn, s1, 5))
	_, err := os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(err)
	_, err = store.NewFileOp().AcceptState(s1).GetFileStat(fn)
	require.NoError(err)
	ok := store.fileMap.Contains(fn)
	require.True(ok)
	_, err = store.NewFileOp().AcceptState(s1).SetFileMetadata(fn, m)
	require.NoError(err)

	// Recreate store nukes store's in memory map
	storeBundle.recreateStore()
	store = storeBundle.store
	ok = store.fileMap.Contains(fn)
	require.False(ok)

	// GetFileReader should load file from disk into map, including metadata.
	_, err = store.NewFileOp().AcceptState(s1).GetFileReader(fn)
	require.NoError(err)
	ok = store.fileMap.Contains(fn)
	require.True(ok)
	result := getMockMetadataOne()
	require.NoError(store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, result))
	require.Equal(m.content, result.content)
}

func testMoveFile(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	s2 := storeBundle.state2
	s3 := storeBundle.state3
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}

	// Update content
	readWriterState2, err := store.NewFileOp().AcceptState(s1).GetFileReadWriter(fn)
	require.NoError(err)
	_, err = readWriterState2.Write([]byte{'t', 'e', 's', 't', '\n'})
	require.NoError(err)
	readWriterState2.Close()
	readWriterState2, err = store.NewFileOp().AcceptState(s1).GetFileReadWriter(fn)
	require.NoError(err)

	// Move from state1 to state2
	err = store.NewFileOp().AcceptState(s1).MoveFile(fn, s2)
	require.NoError(err)
	_, err = os.Stat(filepath.Join(s2.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(err)
	_, err = os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.True(os.IsNotExist(err))
	_, err = store.NewFileOp().AcceptState(s2).GetFileReader(fn)
	require.NoError(err)

	// Move from state1 to state3 would fail with state error
	err = store.NewFileOp().AcceptState(s1).MoveFile(fn, s3)
	require.Error(err)
	require.True(IsFileStateError(err))

	// Create new readWriter at new state
	readWriterState1, err := store.NewFileOp().AcceptState(s2).GetFileReadWriter(fn)
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
	_, err = os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.True(os.IsNotExist(err))
	readWriterState1.Close()
	_, err = os.Stat(filepath.Join(s2.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(err)
	// Check content again
	readWriterStateMoved, err := store.NewFileOp().AcceptState(s2).GetFileReadWriter(fn)
	require.NoError(err)
	dataMoved, err := ioutil.ReadAll(readWriterStateMoved)
	require.NoError(err)
	require.Equal([]byte{'1', 'e', 's', 't', '\n'}, dataMoved)
	readWriterStateMoved.Close()

	// Move back to state1
	err = store.NewFileOp().AcceptState(s2).MoveFile(fn, s1)
	require.NoError(err)
	_, err = store.NewFileOp().AcceptState(s1).GetFileReader(fn)
	require.NoError(err)
}

func testLinkFileTo(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	s3 := storeBundle.state3
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}

	dst := filepath.Join(s3.GetDirectory(), "test_dst")
	require.NoError(store.NewFileOp().AcceptState(s1).LinkFileTo(fn, dst))
	_, err := os.Stat(dst)
	require.NoError(err)
}

func testDeleteFile(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}
	content := "this a test for read after delete"

	// Write to file
	rw, err := store.NewFileOp().AcceptState(s1).GetFileReadWriter(fn)
	require.NoError(err)
	rw.Write([]byte(content))

	// Confirm deletion
	err = store.NewFileOp().AcceptState(s1).DeleteFile(fn)
	require.NoError(err)
	_, err = os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
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
	_, err = store.NewFileOp().AcceptState(s1).GetFileReader(fn)
	require.True(os.IsNotExist(err))
}

func testGetFileReader(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}

	// Get ReadWriter and modify the file.
	readWriter, err := store.NewFileOp().AcceptState(s1).GetFileReadWriter(fn)
	require.NoError(err)
	defer readWriter.Close()
	_, err = readWriter.Write([]byte{'t', 'e', 's', 't', '\n'})
	require.NoError(err)

	// Test getFileReader.
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader, err := store.NewFileOp().AcceptState(s1).GetFileReader(fn)
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
	wg.Wait()

	reader, err := store.NewFileOp().AcceptState(s1).GetFileReader(fn)
	require.NoError(err)
	reader.Close()
}

func testGetFileReadWriter(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}

	// Get ReadWriter and modify file concurrently.
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readWriter, err := store.NewFileOp().AcceptState(s1).GetFileReadWriter(fn)
			require.NoError(err)

			_, err = readWriter.Write([]byte{'t', 'e', 's', 't', '\n'})
			require.NoError(err)

			b := make([]byte, 3)
			_, err = readWriter.Seek(1, 0)
			require.NoError(err)
			l, err := readWriter.Read(b)
			require.NoError(err)
			require.Equal(l, 3)
			require.Equal(string(b[:l]), "est")
			_, err = readWriter.Seek(0, 0)
			require.NoError(err)

			err = readWriter.Close()
			require.NoError(err)
		}()
	}
	wg.Wait()

	// Verify content.
	reader, err := store.NewFileOp().AcceptState(s1).GetFileReader(fn)
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
}

func testGetOrSetFileMetadataConcurrently(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}

	original := []byte("foo")

	// Get ReadWriter and modify file concurrently.
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			m := getMockMetadataOne()
			m.content = original
			require.NoError(store.NewFileOp().AcceptState(s1).GetOrSetFileMetadata(fn, m))
			require.Equal(original, m.content)
		}()
	}
	wg.Wait()

	// Verify content
	m := getMockMetadataOne()
	require.NoError(store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, m))
	require.Equal(original, m.content)
}

func testSetFileMetadataAtConcurrently(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}

	m := getMockMetadataOne()
	m.content = make([]byte, 50)
	updated, err := store.NewFileOp().AcceptState(s1).SetFileMetadata(fn, m)
	require.True(updated)
	require.NoError(err)

	// Get ReadWriter and modify file concurrently.
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()

			_, err := store.NewFileOp().AcceptState(s1).SetFileMetadataAt(fn, m, []byte("f"), int64(offset))
			// require.True(ok)
			require.NoError(err)
		}(i)
	}
	wg.Wait()

	// Verify content
	require.NoError(store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, m))
	require.Equal(50, len(m.content))
	for i := 0; i < 50; i++ {
		require.Equal(byte('f'), m.content[i])
	}
}

func testDeleteFileMetadata(require *require.Assertions, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}

	m := getMockMetadataOne()

	// DeleteFileMetadata doesn't return error if the file doesn't exist.
	require.NoError(store.NewFileOp().AcceptState(s1).DeleteFileMetadata(fn, m))

	m.content = make([]byte, 1)
	updated, err := store.NewFileOp().AcceptState(s1).SetFileMetadata(fn, m)
	require.True(updated)
	require.NoError(err)

	require.NoError(store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, m))
	require.NoError(store.NewFileOp().AcceptState(s1).DeleteFileMetadata(fn, m))
	require.Error(store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, m))
}
