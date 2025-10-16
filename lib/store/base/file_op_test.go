// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package base

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
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

	tests := []func(t *testing.T, storeBundle *fileStoreTestBundle){
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
					s, cleanup := store.fixture()
					defer cleanup()
					test(t, s)
				})
			}
		})
	}
}

func testCreateFile(t *testing.T, storeBundle *fileStoreTestBundle) {
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

	require.Equal(t, successCount, uint32(1))
	require.Equal(t, existsErrorCount, uint32(99))

	// Verify file exists.
	_, err := os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(t, err)

	// Create file again with different target state, but include state of existing file as an acceptable state.
	err = store.NewFileOp().AcceptState(s1).CreateFile(fn, s2, 5)
	require.Error(t, err)
	require.True(t, os.IsExist(err))
	_, err = os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(t, err)
}

func testCreateFileFail(t *testing.T, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	fn := core.DigestFixture().Hex()
	s1 := storeBundle.state1
	s2 := storeBundle.state2
	s3 := storeBundle.state3

	// Create empty file
	err := store.NewFileOp().AcceptState(s1).CreateFile(fn, s1, 5)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(t, err)

	// Create file again with different target state
	err = store.NewFileOp().AcceptState(s3).CreateFile(fn, s2, 5)
	require.Error(t, err)
	require.True(t, IsFileStateError(err))
	require.True(t, strings.HasPrefix(err.Error(), "failed to perform"))
	_, err = os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(t, err)
}

func testReloadFileEntry(t *testing.T, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	fn := core.DigestFixture().Hex()
	m := getMockMetadataOne()
	m.content = []byte("foo")
	s1 := storeBundle.state1

	// Create file
	require.NoError(t, store.NewFileOp().CreateFile(fn, s1, 5))
	_, err := os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(t, err)
	_, err = store.NewFileOp().AcceptState(s1).GetFileStat(fn)
	require.NoError(t, err)
	ok := store.fileMap.Contains(fn)
	require.True(t, ok)
	_, err = store.NewFileOp().AcceptState(s1).SetFileMetadata(fn, m)
	require.NoError(t, err)

	// Recreate store nukes store's in memory map
	storeBundle.recreateStore()
	store = storeBundle.store
	ok = store.fileMap.Contains(fn)
	require.False(t, ok)

	// GetFileReader should load file from disk into map, including metadata.
	_, err = store.NewFileOp().AcceptState(s1).GetFileReader(fn, 0 /* readPartSize */)
	require.NoError(t, err)
	ok = store.fileMap.Contains(fn)
	require.True(t, ok)
	result := getMockMetadataOne()
	require.NoError(t, store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, result))
	require.Equal(t, m.content, result.content)
}

func testMoveFile(t *testing.T, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	s2 := storeBundle.state2
	s3 := storeBundle.state3
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}
	partSize := 100
	// Update content
	readWriterState2, err := store.NewFileOp().AcceptState(s1).GetFileReadWriter(fn, partSize, partSize)
	require.NoError(t, err)
	_, err = readWriterState2.Write([]byte{'t', 'e', 's', 't', '\n'})
	require.NoError(t, err)
	require.NoError(t, readWriterState2.Close())
	readWriterState2, err = store.NewFileOp().AcceptState(s1).GetFileReadWriter(fn, partSize, partSize)
	require.NoError(t, err)

	// Move from state1 to state2
	err = store.NewFileOp().AcceptState(s1).MoveFile(fn, s2)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(s2.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.True(t, os.IsNotExist(err))
	_, err = store.NewFileOp().AcceptState(s2).GetFileReader(fn, partSize)
	require.NoError(t, err)

	// Move from state1 to state3 would fail with state error
	err = store.NewFileOp().AcceptState(s1).MoveFile(fn, s3)
	require.Error(t, err)
	require.True(t, IsFileStateError(err))

	// Create new readWriter at new state
	readWriterState1, err := store.NewFileOp().AcceptState(s2).GetFileReadWriter(fn, partSize, partSize)
	require.NoError(t, err)
	// Check content
	dataState1, err := io.ReadAll(readWriterState1)
	require.NoError(t, err)
	dataState2, err := io.ReadAll(readWriterState2)
	require.NoError(t, err)
	require.Equal(t, dataState1, dataState2)
	require.Equal(t, []byte{'t', 'e', 's', 't', '\n'}, dataState1)
	// Write with old readWriter
	_, err = readWriterState1.WriteAt([]byte{'1'}, 0)
	require.NoError(t, err)
	// Check content again
	_, err = readWriterState1.Seek(0, 0)
	require.NoError(t, err)
	_, err = readWriterState2.Seek(0, 0)
	require.NoError(t, err)
	dataState1, err = io.ReadAll(readWriterState1)
	require.NoError(t, err)
	dataState2, err = io.ReadAll(readWriterState2)
	require.NoError(t, err)
	require.Equal(t, dataState1, dataState2)
	require.Equal(t, []byte{'1', 'e', 's', 't', '\n'}, dataState1)
	// Close on last opened readwriter removes hardlink
	require.NoError(t, readWriterState2.Close())
	_, err = os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.True(t, os.IsNotExist(err))
	require.NoError(t, readWriterState1.Close())
	_, err = os.Stat(filepath.Join(s2.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(t, err)
	// Check content again
	readWriterStateMoved, err := store.NewFileOp().AcceptState(s2).GetFileReadWriter(fn, partSize, partSize)
	require.NoError(t, err)
	dataMoved, err := io.ReadAll(readWriterStateMoved)
	require.NoError(t, err)
	require.Equal(t, []byte{'1', 'e', 's', 't', '\n'}, dataMoved)
	require.NoError(t, readWriterStateMoved.Close())

	// Move back to state1
	err = store.NewFileOp().AcceptState(s2).MoveFile(fn, s1)
	require.NoError(t, err)
	_, err = store.NewFileOp().AcceptState(s1).GetFileReader(fn, partSize)
	require.NoError(t, err)
}

func testLinkFileTo(t *testing.T, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	s3 := storeBundle.state3
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}

	dst := filepath.Join(s3.GetDirectory(), "test_dst")
	require.NoError(t, store.NewFileOp().AcceptState(s1).LinkFileTo(fn, dst))
	_, err := os.Stat(dst)
	require.NoError(t, err)
}

func testDeleteFile(t *testing.T, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}
	content := "this a test for read after delete"

	// Write to file
	rw, err := store.NewFileOp().AcceptState(s1).GetFileReadWriter(fn, 100 /*readPartSize*/, 100 /*writePartSize*/)
	require.NoError(t, err)
	_, err = rw.Write([]byte(content))
	require.NoError(t, err)

	// Confirm deletion
	err = store.NewFileOp().AcceptState(s1).DeleteFile(fn)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.True(t, os.IsNotExist(err))

	// Existing readwriter should still work after deletion
	_, err = rw.Seek(0, 0)
	require.NoError(t, err)
	data, err := io.ReadAll(rw)
	require.NoError(t, err)
	require.Equal(t, content, string(data))

	_, err = rw.Write([]byte(content))
	require.NoError(t, err)
	_, err = rw.Seek(0, 0)
	require.NoError(t, err)
	data, err = io.ReadAll(rw)
	require.NoError(t, err)
	require.Equal(t, content+content, string(data))

	require.NoError(t, rw.Close())

	// Get deleted file should fail
	_, err = store.NewFileOp().AcceptState(s1).GetFileReader(fn, 100 /*readPartSize */)
	require.True(t, os.IsNotExist(err))
}

func testGetFileReader(t *testing.T, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}

	// Get ReadWriter and modify the file.
	readWriter, err := store.NewFileOp().AcceptState(s1).GetFileReadWriter(fn, 100 /*readPartSize */, 100 /*writePartSize*/)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, readWriter.Close())
	}()
	_, err = readWriter.Write([]byte{'t', 'e', 's', 't', '\n'})
	require.NoError(t, err)

	// Test getFileReader.
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader, err := store.NewFileOp().AcceptState(s1).GetFileReader(fn, 100 /*readPartSize */)
			require.NoError(t, err)

			b := make([]byte, 5)
			_, err = reader.Seek(0, 0)
			require.NoError(t, err)
			l, err := reader.ReadAt(b, 0)
			require.NoError(t, err)
			require.Equal(t, l, 5)
			require.Equal(t, string(b[:l]), "test\n")

			err = reader.Close()
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	reader, err := store.NewFileOp().AcceptState(s1).GetFileReader(fn, 100 /*readPartSize */)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
}

func testGetFileReadWriter(t *testing.T, storeBundle *fileStoreTestBundle) {
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
			readWriter, err := store.NewFileOp().AcceptState(s1).GetFileReadWriter(fn, 100 /*readPartSize */, 100 /*writePartSize*/)
			require.NoError(t, err)

			_, err = readWriter.Write([]byte{'t', 'e', 's', 't', '\n'})
			require.NoError(t, err)

			b := make([]byte, 3)
			_, err = readWriter.Seek(1, 0)
			require.NoError(t, err)
			l, err := readWriter.Read(b)
			require.NoError(t, err)
			require.Equal(t, l, 3)
			require.Equal(t, string(b[:l]), "est")
			_, err = readWriter.Seek(0, 0)
			require.NoError(t, err)

			err = readWriter.Close()
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	// Verify content.
	reader, err := store.NewFileOp().AcceptState(s1).GetFileReader(fn, 100 /*readPartSize */)
	require.NoError(t, err)

	b := make([]byte, 5)
	_, err = reader.Seek(0, 0)
	require.NoError(t, err)
	l, err := reader.ReadAt(b, 0)
	require.NoError(t, err)
	require.Equal(t, l, 5)
	require.Equal(t, string(b[:l]), "test\n")

	err = reader.Close()
	require.NoError(t, err)
}

func testGetOrSetFileMetadataConcurrently(t *testing.T, storeBundle *fileStoreTestBundle) {
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
			require.NoError(t, store.NewFileOp().AcceptState(s1).GetOrSetFileMetadata(fn, m))
			require.Equal(t, original, m.content)
		}()
	}
	wg.Wait()

	// Verify content
	m := getMockMetadataOne()
	require.NoError(t, store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, m))
	require.Equal(t, original, m.content)
}

func testSetFileMetadataAtConcurrently(t *testing.T, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}

	m := getMockMetadataOne()
	m.content = make([]byte, 50)
	updated, err := store.NewFileOp().AcceptState(s1).SetFileMetadata(fn, m)
	require.True(t, updated)
	require.NoError(t, err)

	// Get ReadWriter and modify file concurrently.
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()

			_, err := store.NewFileOp().AcceptState(s1).SetFileMetadataAt(fn, m, []byte("f"), int64(offset))
			// require.True(ok)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	// Verify content
	require.NoError(t, store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, m))
	require.Equal(t, 50, len(m.content))
	for i := 0; i < 50; i++ {
		require.Equal(t, byte('f'), m.content[i])
	}
}

func testDeleteFileMetadata(t *testing.T, storeBundle *fileStoreTestBundle) {
	store := storeBundle.store

	s1 := storeBundle.state1
	fn, ok := storeBundle.files[s1]
	if !ok {
		log.Fatal("file not found in state1")
	}

	m := getMockMetadataOne()

	// DeleteFileMetadata doesn't return error if the file doesn't exist.
	require.NoError(t, store.NewFileOp().AcceptState(s1).DeleteFileMetadata(fn, m))

	m.content = make([]byte, 1)
	updated, err := store.NewFileOp().AcceptState(s1).SetFileMetadata(fn, m)
	require.True(t, updated)
	require.NoError(t, err)

	require.NoError(t, store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, m))
	require.NoError(t, store.NewFileOp().AcceptState(s1).DeleteFileMetadata(fn, m))
	require.Error(t, store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, m))
}
