package base

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadMetadataAndWriteMetadata(t *testing.T) {
	cleanupTestFileEntry()
	fe, err := getTestFileEntry()
	assert.Nil(t, err)
	defer cleanupTestFileEntry()

	m1 := getMockMetadataOne()
	b2 := make([]byte, 2)

	// Invalid read
	_, err = fe.ReadMetadata(dummyVerify, m1)
	assert.True(t, os.IsNotExist(err))

	// Write metadata
	updated, err := fe.WriteMetadata(dummyVerify, m1, []byte{uint8(0), uint8(0)})
	assert.Nil(t, err)
	assert.True(t, updated)

	updated, err = fe.WriteMetadata(dummyVerify, getMockMetadataOne(), []byte{uint8(0), uint8(0)})
	assert.Nil(t, err)
	assert.False(t, updated)

	// Read metadata
	b2, err = fe.ReadMetadata(dummyVerify, m1)
	assert.Nil(t, err)
	assert.NotNil(t, b2)
	assert.Equal(t, uint8(0), b2[0])
	assert.Equal(t, uint8(0), b2[1])

	// Invalid read
	b2, err = fe.ReadMetadata(dummyVerify, getMockMetadataTwo())
	assert.True(t, os.IsNotExist(err))
}

func TestReadMetadataAtAndWriteMetadataAt(t *testing.T) {
	cleanupTestFileEntry()
	fe, err := getTestFileEntry()
	assert.Nil(t, err)
	defer cleanupTestFileEntry()

	m1 := getMockMetadataOne()
	b1 := make([]byte, 1)
	b2 := make([]byte, 2)

	// Invalid write at
	n, err := fe.WriteMetadataAt(dummyVerify, m1, b2, 0)
	assert.NotNil(t, err)
	assert.Equal(t, n, 0)

	// Write metadata
	updated, err := fe.WriteMetadata(dummyVerify, m1, []byte{uint8(0), uint8(0)})
	assert.Nil(t, err)
	assert.True(t, updated)

	// Write metadata at
	n, err = fe.WriteMetadataAt(dummyVerify, m1, []byte{uint8(1)}, 1)
	assert.Nil(t, err)
	assert.Equal(t, n, 1)

	n, err = fe.WriteMetadataAt(dummyVerify, getMockMetadataOne(), []byte{uint8(1)}, 1)
	assert.Nil(t, err)
	assert.Equal(t, n, 0)

	// Read metadata at
	b2 = make([]byte, 2)
	b1 = make([]byte, 1)
	n, err = fe.ReadMetadataAt(dummyVerify, m1, b1, 0)
	assert.Nil(t, err)
	assert.Equal(t, n, 1)
	assert.Equal(t, uint8(0), b1[0])

	n, err = fe.ReadMetadataAt(dummyVerify, m1, b1, 1)
	assert.Nil(t, err)
	assert.Equal(t, n, 1)
	assert.Equal(t, uint8(1), b1[0])

	n, err = fe.ReadMetadataAt(dummyVerify, m1, b2, 1)
	assert.NotNil(t, err)
	assert.Equal(t, n, 1)
	assert.Equal(t, uint8(1), b2[0])

	// Concurrent write at and read at
	b100 := make([]byte, 100)
	updated, err = fe.WriteMetadata(dummyVerify, m1, b100)

	wg := &sync.WaitGroup{}
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			value := rand.Intn(254) + 1
			bb1 := make([]byte, 1)

			// Write at
			m, e := fe.WriteMetadataAt(dummyVerify, m1, []byte{byte(value)}, int64(i))
			assert.Nil(t, e)
			assert.Equal(t, m, 1)

			m, e = fe.WriteMetadataAt(dummyVerify, getMockMetadataOne(), []byte{byte(value)}, int64(i))
			assert.Nil(t, e)
			assert.Equal(t, m, 0)

			// Read at
			m, e = fe.ReadMetadataAt(dummyVerify, m1, bb1, int64(i))
			assert.Nil(t, e)
			assert.Equal(t, m, 1)
			assert.Equal(t, byte(value), bb1[0])

			wg.Done()

		}(i)
	}
	wg.Wait()
}

func TestReload(t *testing.T) {
	cleanupTestFileStore()
	s, err := getTestFileStore()
	assert.Nil(t, err)
	defer cleanupTestFileStore()

	err = s.CreateFile(testFileName, []FileState{}, stateTest1, 5)
	assert.Nil(t, err)
	fe, _, err := s.LoadFileEntry(testFileName, []FileState{stateTest1})
	assert.Nil(t, err)

	m1 := getMockMetadataOne()
	b2 := make([]byte, 2)

	// Write metadata
	updated, err := fe.WriteMetadata(dummyVerify, m1, []byte{uint8(0), uint8(1)})
	assert.Nil(t, err)
	assert.True(t, updated)

	// Reload
	NewLocalFileStore(&LocalFileEntryInternalFactory{}, &LocalFileEntryFactory{})
	s.GetFileStat(testFileName, []FileState{stateTest1})
	fe, _, _ = s.LoadFileEntry(testFileName, []FileState{stateTest1})

	// Read metadata
	b2, err = fe.ReadMetadata(dummyVerify, m1)
	assert.Nil(t, err)
	assert.NotNil(t, b2)
	assert.Equal(t, uint8(0), b2[0])
	assert.Equal(t, uint8(1), b2[1])

	// Invalid read metadata
	b2, err = fe.ReadMetadata(dummyVerify, getMockMetadataTwo())
	assert.True(t, os.IsNotExist(err))

	// Write metadata
	updated, err = fe.WriteMetadata(dummyVerify, m1, []byte{uint8(1), uint8(1)})
	assert.Nil(t, err)
	assert.True(t, updated)
	b2, err = fe.ReadMetadata(dummyVerify, m1)
	assert.Nil(t, err)
	assert.NotNil(t, b2)

	// Read metadata from disk directly
	fp, _ := fe.GetPath(dummyVerify)
	content, err := ioutil.ReadFile(fp + getMockMetadataOne().GetSuffix())
	assert.Nil(t, err)
	assert.Equal(t, uint8(1), content[0])
	assert.Equal(t, uint8(1), content[1])
}

func TestMove(t *testing.T) {
	cleanupTestFileStore()
	s, err := getTestFileStore()
	assert.Nil(t, err)
	defer cleanupTestFileStore()

	err = s.CreateFile(testFileName, []FileState{}, stateTest1, 5)
	assert.Nil(t, err)
	fe, _, err := s.LoadFileEntry(testFileName, []FileState{stateTest1})
	assert.Nil(t, err)

	m1 := getMockMetadataOne()
	b1 := make([]byte, 2)
	m2 := getMockMetadataMovable()
	b2 := make([]byte, 1)

	// Write metadata
	updated, err := fe.WriteMetadata(dummyVerify, m1, []byte{uint8(0), uint8(1)})
	assert.Nil(t, err)
	assert.True(t, updated)
	updated, err = fe.WriteMetadata(dummyVerify, m2, []byte{uint8(3)})
	assert.Nil(t, err)
	assert.True(t, updated)

	// Read metadata
	b1, err = fe.ReadMetadata(dummyVerify, m1)
	assert.Nil(t, err)
	assert.NotNil(t, b1)
	assert.Equal(t, uint8(0), b1[0])
	assert.Equal(t, uint8(1), b1[1])
	b2, err = fe.ReadMetadata(dummyVerify, m2)
	assert.Nil(t, err)
	assert.NotNil(t, b2)
	assert.Equal(t, uint8(3), b2[0])

	// Move file, removes non-movable metadata.
	err = s.MoveFile(testFileName, []FileState{stateTest1}, stateTest3)
	assert.Nil(t, err)
	fe, _, _ = s.LoadFileEntry(testFileName, []FileState{stateTest3})

	// Verify metadata is gone
	_, err = fe.ReadMetadata(dummyVerify, m1)
	assert.NotNil(t, err)
	assert.True(t, os.IsNotExist(err))

	// Verify metadataMovable still exists
	b2Moved, err := fe.ReadMetadata(dummyVerify, m2)
	assert.Nil(t, err)
	assert.NotNil(t, b2Moved)
	assert.Equal(t, uint8(3), b2Moved[0])

	// Verify file location
	_, err = os.Stat(path.Join(stateTest1.GetDirectory(), testFileName+getMockMetadataOne().GetSuffix()))
	assert.NotNil(t, err)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(path.Join(stateTest2.GetDirectory(), testFileName+getMockMetadataOne().GetSuffix()))
	assert.NotNil(t, err)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(path.Join(stateTest3.GetDirectory(), testFileName+getMockMetadataOne().GetSuffix()))
	assert.NotNil(t, err)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(path.Join(stateTest3.GetDirectory(), testFileName))
	assert.Nil(t, err)
	_, err = os.Stat(path.Join(stateTest1.GetDirectory(), testFileName+getMockMetadataMovable().GetSuffix()))
	assert.NotNil(t, err)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(path.Join(stateTest2.GetDirectory(), testFileName+getMockMetadataMovable().GetSuffix()))
	assert.NotNil(t, err)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(path.Join(stateTest3.GetDirectory(), testFileName+getMockMetadataMovable().GetSuffix()))
	assert.Nil(t, err)
}

func TestDelete(t *testing.T) {
	cleanupTestFileEntry()
	fe, err := getTestFileEntry()
	assert.Nil(t, err)
	defer cleanupTestFileEntry()

	m1 := getMockMetadataOne()

	// Write metadata
	updated, err := fe.WriteMetadata(dummyVerify, m1, []byte{uint8(0), uint8(0)})
	assert.Nil(t, err)
	assert.True(t, updated)
	_, e := fe.ReadMetadata(dummyVerify, m1)
	assert.Nil(t, e)

	// Delete
	_, err = os.Stat(path.Join(stateTest1.GetDirectory(), testFileName+getMockMetadataOne().GetSuffix()))
	assert.Nil(t, err)
	err = fe.DeleteMetadata(dummyVerify, m1)
	assert.Nil(t, err)
	_, err = os.Stat(path.Join(stateTest1.GetDirectory(), testFileName+getMockMetadataOne().GetSuffix()))
	assert.NotNil(t, err)
}
