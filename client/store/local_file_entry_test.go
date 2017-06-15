package store

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/syncmap"
)

type mockMetadata struct {
	randomSuffix string
}

func getMockMetadataOne() MetadataType {
	return mockMetadata{
		randomSuffix: "_suffix/one",
	}
}

func getMockMetadataTwo() MetadataType {
	return mockMetadata{
		randomSuffix: "_suffix/two",
	}
}

func getMockMetadataFromPath(fp string) MetadataType {
	if strings.HasSuffix(fp, getMockMetadataOne().Suffix()) {
		return getMockMetadataOne()
	}
	if strings.HasSuffix(fp, getMockMetadataTwo().Suffix()) {
		return getMockMetadataTwo()
	}
	return nil
}

func (m mockMetadata) Suffix() string {
	return m.randomSuffix
}

func (m mockMetadata) IsValidState(state FileState) bool {
	switch state {
	case stateTest1:
		return true
	case stateTest2:
		return true
	default:
		return false
	}
}

func (m *mockMetadata) Set(file FileEntry, content []byte) (bool, error) {
	return false, nil
}
func (m *mockMetadata) Get(file FileEntry) ([]byte, error) {
	return nil, nil
}
func (m *mockMetadata) Delete(file FileEntry) error {
	return nil
}

func getTestFileEntry() (*localFileStoreBackend, FileEntry, error) {
	// Setup
	var testFileName = "test_file.txt"
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

	// Create empty file
	backend := &localFileStoreBackend{
		fileMap: &syncmap.Map{},
	}
	_, err := backend.CreateFile(testFileName, []FileState{}, stateTest1, 5)
	if err != nil {
		return nil, nil, err
	}

	// Register mock metadata type
	_testMetadataLookupFuncs = append(_testMetadataLookupFuncs, getMockMetadataFromPath)

	entry, _ := backend.fileMap.Load(testFileName)
	return backend, entry.(FileEntry), nil
}

func cleanupTestFileEntry() {
	os.RemoveAll(_testRoot)
}

func TestMetadata(t *testing.T) {
	cleanupTestFileEntry()
	backend, fe, err := getTestFileEntry()
	defer cleanupTestFileEntry()

	m1 := getMockMetadataOne()
	b := make([]byte, 2)
	b1 := make([]byte, 1)

	// Invalid get
	_, err = fe.ReadMetadata(nil, m1)
	assert.True(t, os.IsNotExist(err))

	// Invalid write at
	n, err := fe.WriteMetadataAt(nil, m1, b, 0)
	assert.NotNil(t, err)
	assert.Equal(t, n, 0)

	// Set all
	updated, err := fe.WriteMetadata(nil, m1, []byte{PieceClean, PieceClean})
	assert.Nil(t, err)
	assert.True(t, updated)

	updated, err = fe.WriteMetadata(nil, getMockMetadataOne(), []byte{PieceClean, PieceClean})
	assert.Nil(t, err)
	assert.False(t, updated)

	// Get all
	b, err = fe.ReadMetadata(nil, m1)
	assert.Nil(t, err)
	assert.NotNil(t, b)
	assert.Equal(t, PieceClean, b[0])
	assert.Equal(t, PieceClean, b[1])

	// Invalid get
	b, err = fe.ReadMetadata(nil, getMockMetadataTwo())
	assert.True(t, os.IsNotExist(err))

	// Write at
	n, err = fe.WriteMetadataAt(nil, m1, []byte{PieceDirty}, 1)
	assert.Nil(t, err)
	assert.Equal(t, n, 1)

	n, err = fe.WriteMetadataAt(nil, getMockMetadataOne(), []byte{PieceDirty}, 1)
	assert.Nil(t, err)
	assert.Equal(t, n, 0)

	// Read at
	b = make([]byte, 2)
	b1 = make([]byte, 1)
	n, err = fe.ReadMetadataAt(nil, m1, b1, 0)
	assert.Nil(t, err)
	assert.Equal(t, n, 1)
	assert.Equal(t, PieceClean, b1[0])

	n, err = fe.ReadMetadataAt(nil, m1, b1, 1)
	assert.Nil(t, err)
	assert.Equal(t, n, 1)
	assert.Equal(t, PieceDirty, b1[0])

	n, err = fe.ReadMetadataAt(nil, m1, b, 1)
	assert.NotNil(t, err)
	assert.Equal(t, n, 1)
	assert.Equal(t, PieceDirty, b1[0])

	// Move
	err = backend.MoveFile("test_file.txt", []FileState{stateTest1}, stateTest2)
	assert.Nil(t, err)

	_, err = os.Stat(path.Join(stateTest1.GetDirectory(), "test_file.txt"+getMockMetadataOne().Suffix()))
	assert.NotNil(t, err)
	_, err = os.Stat(path.Join(stateTest2.GetDirectory(), "test_file.txt"+getMockMetadataOne().Suffix()))
	assert.Nil(t, err)
	_, err = os.Stat(path.Join(stateTest3.GetDirectory(), "test_file.txt"+getMockMetadataOne().Suffix()))
	assert.NotNil(t, err)
	b, err = fe.ReadMetadata(nil, m1)
	assert.Nil(t, err)
	assert.NotNil(t, b)
	assert.Equal(t, PieceClean, b[0])
	assert.Equal(t, PieceDirty, b[1])

	// Reload
	backend = &localFileStoreBackend{
		fileMap: &syncmap.Map{},
	}
	backend.GetFileStat("test_file.txt", []FileState{stateTest2})
	entry, _ := backend.fileMap.Load("test_file.txt")
	fe = entry.(FileEntry)

	// Get all
	b, err = fe.ReadMetadata(nil, m1)
	assert.Nil(t, err)
	assert.NotNil(t, b)
	assert.Equal(t, PieceClean, b[0])
	assert.Equal(t, PieceDirty, b[1])

	// Invalid get.
	b, err = fe.ReadMetadata(nil, getMockMetadataTwo())
	assert.True(t, os.IsNotExist(err))

	// Set all
	updated, err = fe.WriteMetadata(nil, m1, []byte{PieceDirty, PieceDirty})
	b, err = fe.ReadMetadata(nil, m1)
	assert.Nil(t, err)
	assert.True(t, updated)

	// Get all
	b, err = fe.ReadMetadata(nil, m1)
	assert.Nil(t, err)
	assert.NotNil(t, b)
	assert.Equal(t, PieceDirty, b[0])
	assert.Equal(t, PieceDirty, b[1])

	fp, _ := fe.GetPath(nil)
	content, err := ioutil.ReadFile(fp + getMockMetadataOne().Suffix())
	assert.Nil(t, err)
	assert.Equal(t, PieceDirty, content[0])
	assert.Equal(t, PieceDirty, content[1])

	// Write at
	n, err = fe.WriteMetadataAt(nil, m1, []byte{PieceDone}, 0)
	assert.Nil(t, err)
	assert.Equal(t, n, 1)

	n, err = fe.WriteMetadataAt(nil, m1, []byte{PieceDone}, 0)
	assert.Nil(t, err)
	assert.Equal(t, n, 0)

	// Read at
	n, err = fe.ReadMetadataAt(nil, m1, b1, 0)
	assert.Nil(t, err)
	assert.Equal(t, n, 1)
	assert.Equal(t, PieceDone, b1[0])

	n, err = fe.ReadMetadataAt(nil, m1, b1, 1)
	assert.Nil(t, err)
	assert.Equal(t, n, 1)
	assert.Equal(t, PieceDirty, b1[0])

	n, err = fe.ReadMetadataAt(nil, m1, b, 1)
	assert.NotNil(t, err)
	assert.Equal(t, n, 1)
	assert.Equal(t, PieceDirty, b1[0])

	// Move file to invalid state
	err = backend.MoveFile("test_file.txt", []FileState{stateTest2}, stateTest3)
	assert.Nil(t, err)
	entry, _ = backend.fileMap.Load("test_file.txt")
	fe = entry.(FileEntry)

	b, err = fe.ReadMetadata(nil, m1)
	assert.NotNil(t, err)

	_, err = os.Stat(path.Join(stateTest1.GetDirectory(), "test_file.txt"+getMockMetadataOne().Suffix()))
	assert.NotNil(t, err)
	_, err = os.Stat(path.Join(stateTest2.GetDirectory(), "test_file.txt"+getMockMetadataOne().Suffix()))
	assert.NotNil(t, err)
	_, err = os.Stat(path.Join(stateTest3.GetDirectory(), "test_file.txt"+getMockMetadataOne().Suffix()))
	assert.NotNil(t, err)

	// Read and Write concurrently
	backend.MoveFile("test_file.txt", []FileState{stateTest3}, stateTest1)
	entry, _ = backend.fileMap.Load("test_file.txt")
	fe = entry.(FileEntry)
	b100 := make([]byte, 100)
	updated, err = fe.WriteMetadata(nil, m1, b100)
	assert.Nil(t, err)
	assert.True(t, updated)

	wg := &sync.WaitGroup{}
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			value := rand.Intn(254) + 1
			bb1 := make([]byte, 1)

			// Write at
			m, e := fe.WriteMetadataAt(nil, m1, []byte{byte(value)}, int64(i))
			assert.Nil(t, e)
			assert.Equal(t, m, 1)

			m, e = fe.WriteMetadataAt(nil, getMockMetadataOne(), []byte{byte(value)}, int64(i))
			assert.Nil(t, e)
			assert.Equal(t, m, 0)

			// Read at
			m, e = fe.ReadMetadataAt(nil, m1, bb1, int64(i))
			assert.Nil(t, e)
			assert.Equal(t, m, 1)
			assert.Equal(t, byte(value), bb1[0])

			wg.Done()

		}(i)
	}
	wg.Wait()

	// Delete
	_, err = os.Stat(path.Join(stateTest3.GetDirectory(), "test_file.txt"+getMockMetadataOne().Suffix()))
	assert.Nil(t, err)
	err = fe.DeleteMetadata(nil, m1)
	assert.Nil(t, err)
	_, err = os.Stat(path.Join(stateTest3.GetDirectory(), "test_file.txt"+getMockMetadataOne().Suffix()))
	assert.NotNil(t, err)
}

func TestRefCount(t *testing.T) {
	cleanupTestFileEntry()
	backend, fe, err := getTestFileEntry()
	defer cleanupTestFileEntry()

	wg := &sync.WaitGroup{}
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func() {
			maxCount := rand.Intn(100) + 1
			var refCount int64
			var err error
			for j := 0; j < maxCount; j++ {
				// Inc
				refCount, err = fe.IncrementRefCount(nil)
				assert.Nil(t, err)
			}
			assert.True(t, refCount >= int64(maxCount))

			// Try Delete
			fileName, _ := fe.GetName(nil)
			err = backend.DeleteFile(fileName, []FileState{stateTest1})
			assert.True(t, IsRefCountError(err))

			for j := 0; j < maxCount; j++ {
				// Dec
				refCount, err = fe.DecrementRefCount(nil)
				assert.Nil(t, err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	refCount, err := fe.GetRefCount(nil)
	assert.Nil(t, err)
	assert.Equal(t, refCount, int64(0))
}
