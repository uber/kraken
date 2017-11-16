package base

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadMetadataAndWriteMetadata(t *testing.T) {
	require := require.New(t)
	storeBundle, cleanup := fileStoreDefaultFixture()
	defer cleanup()

	fileBundle, ok := storeBundle.files[storeBundle.state1]
	if !ok {
		log.Fatal("file not found in state1")
	}
	fe := fileBundle.entry
	verify := fileBundle.verify

	m1 := getMockMetadataOne()
	b2 := make([]byte, 2)

	// Invalid read
	_, err := fe.ReadMetadata(verify, m1)
	require.True(os.IsNotExist(err))

	// Write metadata
	updated, err := fe.WriteMetadata(verify, m1, []byte{uint8(0), uint8(0)})
	require.NoError(err)
	require.True(updated)

	updated, err = fe.WriteMetadata(verify, getMockMetadataOne(), []byte{uint8(0), uint8(0)})
	require.NoError(err)
	require.False(updated)

	// Read metadata
	b2, err = fe.ReadMetadata(verify, m1)
	require.NoError(err)
	require.NotNil(b2)
	require.Equal(uint8(0), b2[0])
	require.Equal(uint8(0), b2[1])

	// Invalid read
	b2, err = fe.ReadMetadata(verify, getMockMetadataTwo())
	require.True(os.IsNotExist(err))
}

func TestReadMetadataAtAndWriteMetadataAt(t *testing.T) {
	require := require.New(t)
	storeBundle, cleanup := fileStoreDefaultFixture()
	defer cleanup()

	fileBundle, ok := storeBundle.files[storeBundle.state1]
	if !ok {
		log.Fatal("file not found in state1")
	}
	fe := fileBundle.entry
	verify := fileBundle.verify

	m1 := getMockMetadataOne()
	b1 := make([]byte, 1)
	b2 := make([]byte, 2)

	// Invalid write at
	n, err := fe.WriteMetadataAt(verify, m1, b2, 0)
	require.NotNil(err)
	require.Equal(n, 0)

	// Write metadata
	updated, err := fe.WriteMetadata(verify, m1, []byte{uint8(0), uint8(0)})
	require.NoError(err)
	require.True(updated)

	// Write metadata at
	n, err = fe.WriteMetadataAt(verify, m1, []byte{uint8(1)}, 1)
	require.NoError(err)
	require.Equal(n, 1)

	n, err = fe.WriteMetadataAt(verify, getMockMetadataOne(), []byte{uint8(1)}, 1)
	require.NoError(err)
	require.Equal(n, 0)

	// Read metadata at
	b2 = make([]byte, 2)
	b1 = make([]byte, 1)
	n, err = fe.ReadMetadataAt(verify, m1, b1, 0)
	require.NoError(err)
	require.Equal(n, 1)
	require.Equal(uint8(0), b1[0])

	n, err = fe.ReadMetadataAt(verify, m1, b1, 1)
	require.NoError(err)
	require.Equal(n, 1)
	require.Equal(uint8(1), b1[0])

	n, err = fe.ReadMetadataAt(verify, m1, b2, 1)
	require.NotNil(err)
	require.Equal(n, 1)
	require.Equal(uint8(1), b2[0])

	// Concurrent write at and read at
	b100 := make([]byte, 100)
	updated, err = fe.WriteMetadata(verify, m1, b100)

	wg := &sync.WaitGroup{}
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			value := rand.Intn(254) + 1
			bb1 := make([]byte, 1)

			// Write at
			m, e := fe.WriteMetadataAt(verify, m1, []byte{byte(value)}, int64(i))
			require.NoError(e)
			require.Equal(m, 1)

			m, e = fe.WriteMetadataAt(verify, getMockMetadataOne(), []byte{byte(value)}, int64(i))
			require.NoError(e)
			require.Equal(m, 0)

			// Read at
			m, e = fe.ReadMetadataAt(verify, m1, bb1, int64(i))
			require.NoError(e)
			require.Equal(m, 1)
			require.Equal(byte(value), bb1[0])

			wg.Done()

		}(i)
	}
	wg.Wait()
}

func TestReload(t *testing.T) {
	require := require.New(t)
	storeBundle, cleanup := fileStoreDefaultFixture()
	defer cleanup()
	store := storeBundle.store

	fileBundle, ok := storeBundle.files[storeBundle.state1]
	if !ok {
		log.Fatal("file not found in state1")
	}
	fe := fileBundle.entry
	verify := fileBundle.verify
	fn := fileBundle.name

	m1 := getMockMetadataOne()
	b2 := make([]byte, 2)

	// Write metadata
	updated, err := fe.WriteMetadata(verify, m1, []byte{uint8(0), uint8(1)})
	require.NoError(err)
	require.True(updated)

	// Reload
	_, err = (&LocalFileStoreBuilder{}).Build()
	require.NoError(err)
	store.GetFileStat(fn, []FileState{storeBundle.state1})
	fe, _, _ = store.LoadFileEntry(fn, []FileState{storeBundle.state1})

	// Read metadata
	b2, err = fe.ReadMetadata(verify, m1)
	require.NoError(err)
	require.NotNil(b2)
	require.Equal(uint8(0), b2[0])
	require.Equal(uint8(1), b2[1])

	// Invalid read metadata
	b2, err = fe.ReadMetadata(verify, getMockMetadataTwo())
	require.True(os.IsNotExist(err))

	// Write metadata
	updated, err = fe.WriteMetadata(verify, m1, []byte{uint8(1), uint8(1)})
	require.NoError(err)
	require.True(updated)
	b2, err = fe.ReadMetadata(verify, m1)
	require.NoError(err)
	require.NotNil(b2)

	// Read metadata from disk directly
	fp, _ := fe.GetPath(verify)
	content, err := ioutil.ReadFile(path.Join(path.Dir(fp), getMockMetadataOne().GetSuffix()))
	require.NoError(err)
	require.Equal(uint8(1), content[0])
	require.Equal(uint8(1), content[1])
}

func TestMove(t *testing.T) {
	require := require.New(t)
	storeBundle, cleanup := fileStoreDefaultFixture()
	defer cleanup()
	store := storeBundle.store

	fileBundle, ok := storeBundle.files[storeBundle.state1]
	if !ok {
		log.Fatal("file not found in state1")
	}
	fe := fileBundle.entry
	verify := fileBundle.verify
	fn := fileBundle.name

	m1 := getMockMetadataOne()
	b1 := make([]byte, 2)
	m2 := getMockMetadataMovable()
	b2 := make([]byte, 1)

	// Write metadata
	updated, err := fe.WriteMetadata(verify, m1, []byte{uint8(0), uint8(1)})
	require.NoError(err)
	require.True(updated)
	updated, err = fe.WriteMetadata(verify, m2, []byte{uint8(3)})
	require.NoError(err)
	require.True(updated)

	// Read metadata
	b1, err = fe.ReadMetadata(verify, m1)
	require.NoError(err)
	require.NotNil(b1)
	require.Equal(uint8(0), b1[0])
	require.Equal(uint8(1), b1[1])
	b2, err = fe.ReadMetadata(verify, m2)
	require.NoError(err)
	require.NotNil(b2)
	require.Equal(uint8(3), b2[0])

	// Move file, removes non-movable metadata.
	err = store.MoveFile(fn, []FileState{storeBundle.state1}, storeBundle.state3)
	require.NoError(err)
	fe, _, _ = store.LoadFileEntry(fn, []FileState{storeBundle.state3})

	// Verify metadata still exists
	_, err = fe.ReadMetadata(verify, m1)
	require.NotNil(err)
	require.True(os.IsNotExist(err))

	// Verify metadata still exists
	b2Moved, err := fe.ReadMetadata(verify, m2)
	require.NoError(err)
	require.NotNil(b2Moved)
	require.Equal(uint8(3), b2Moved[0])

	// Not movable metadata should get deleted after move
	// it should not exist anywhere
	_, err = os.Stat(path.Join(storeBundle.state1.GetDirectory(), fn, getMockMetadataOne().GetSuffix()))
	require.NotNil(err)
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(storeBundle.state2.GetDirectory(), fn, getMockMetadataOne().GetSuffix()))
	require.NotNil(err)
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(storeBundle.state3.GetDirectory(), fn, getMockMetadataOne().GetSuffix()))
	require.NotNil(err)
	require.True(os.IsNotExist(err))

	// Movable metadata will be moved along with the file entry
	_, err = os.Stat(path.Join(storeBundle.state3.GetDirectory(), fn))
	require.Nil(err)
	_, err = os.Stat(path.Join(storeBundle.state1.GetDirectory(), fn, getMockMetadataMovable().GetSuffix()))
	require.NotNil(err)
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(storeBundle.state2.GetDirectory(), fn, getMockMetadataMovable().GetSuffix()))
	require.NotNil(err)
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(storeBundle.state3.GetDirectory(), fn, getMockMetadataMovable().GetSuffix()))
	require.NoError(err)
}

func TestDelete(t *testing.T) {
	require := require.New(t)
	storeBundle, cleanup := fileStoreDefaultFixture()
	defer cleanup()

	fileBundle, ok := storeBundle.files[storeBundle.state1]
	if !ok {
		log.Fatal("file not found in state1")
	}
	fe := fileBundle.entry
	verify := fileBundle.verify
	fn := fileBundle.name

	m1 := getMockMetadataOne()

	// Write metadata
	updated, err := fe.WriteMetadata(verify, m1, []byte{uint8(0), uint8(0)})
	require.NoError(err)
	require.True(updated)
	_, e := fe.ReadMetadata(verify, m1)
	require.NoError(e)

	// Stat metadatafile before and after deletion to ensure that it is deleted
	_, err = os.Stat(path.Join(storeBundle.state1.GetDirectory(), fn, getMockMetadataOne().GetSuffix()))
	require.NoError(err)
	err = fe.DeleteMetadata(verify, m1)
	require.NoError(err)
	_, err = os.Stat(path.Join(storeBundle.state1.GetDirectory(), fn, getMockMetadataOne().GetSuffix()))
	require.NotNil(err)
}
