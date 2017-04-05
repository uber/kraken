package store

import (
	"io/ioutil"
	"sync"
	"testing"

	"path"

	"os"

	"fmt"

	"github.com/stretchr/testify/assert"
)

type _mockFileEntry struct {
	filepath string
	state    FileState
}

func newMockFileEntry(fp string, state FileState) FileEntry {
	return &_mockFileEntry{
		filepath: fp,
		state:    state,
	}
}

func (m *_mockFileEntry) GetName() string                            { return "" }
func (m *_mockFileEntry) GetPath() string                            { return m.filepath }
func (m *_mockFileEntry) GetState() FileState                        { return m.state }
func (m *_mockFileEntry) SetState(state FileState)                   { m.state = state }
func (m *_mockFileEntry) IsOpen() bool                               { return false }
func (m *_mockFileEntry) Stat() (os.FileInfo, error)                 { return nil, nil }
func (m *_mockFileEntry) GetFileReader() (FileReader, error)         { return nil, nil }
func (m *_mockFileEntry) GetFileReadWriter() (FileReadWriter, error) { return nil, nil }

func getTestDir() (string, error) {
	return ioutil.TempDir("./.tmp/", "metadata")
}

func TestPieceStatus(t *testing.T) {
	testDir, err := getTestDir()
	assert.Nil(t, err)
	defer os.RemoveAll(testDir)

	fp := path.Join(testDir, "testPieceStatus")
	fe := newMockFileEntry(fp, stateDownload)
	p0 := getPieceStatus(0, 2)
	p1 := getPieceStatus(1, 2)
	pall := getPieceStatus(-1, 2)

	// get on Nil p0
	_, err = p0.Get(fe)
	assert.True(t, os.IsNotExist(err))
	_, err = pall.Get(fe)
	assert.True(t, os.IsNotExist(err))

	// set invalid content pall
	updated, err := pall.Set(fe, []byte{PieceClean})
	assert.NotNil(t, err)
	assert.False(t, updated)
	assert.Equal(t, "Failed to set piece status. Invalid content: expecting length 2 but got 1.", err.Error())

	// set all
	updated, err = pall.Set(fe, []byte{PieceClean, PieceClean})
	assert.Nil(t, err)
	assert.True(t, updated)

	dall, err := pall.Get(fe)
	assert.Nil(t, err)
	assert.NotNil(t, dall)
	assert.Equal(t, PieceClean, dall[0])
	assert.Equal(t, PieceClean, dall[1])

	updated, err = pall.Set(fe, []byte{PieceClean, PieceClean})
	assert.Nil(t, err)
	assert.False(t, updated)

	updated, err = pall.Set(fe, []byte{PieceDone, PieceClean})
	assert.Nil(t, err)
	assert.True(t, updated)

	// get all
	dall, err = pall.Get(fe)
	assert.Nil(t, err)
	assert.NotNil(t, dall)
	assert.Equal(t, PieceDone, dall[0])
	assert.Equal(t, PieceClean, dall[1])

	fe.SetState(stateCache)
	updated, err = pall.Set(fe, []byte{PieceDirty, PieceDirty})
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Sprintf("Cannot change piece status for %s: %d. File not in download directory.", fp, -1), err.Error())
	assert.False(t, updated)

	dall, err = pall.Get(fe)
	assert.Nil(t, err)
	assert.Equal(t, PieceDone, dall[0])
	assert.Equal(t, PieceDone, dall[1])

	fe.SetState(stateTrash)
	dall, err = pall.Get(fe)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Sprintf("Failed to get piece status for %s: %d cannot find file in download nor cache directory.", fp, -1), err.Error())

	fe.SetState(stateDownload)

	// set on Nil content p0
	updated, err = p0.Set(fe, nil)
	assert.False(t, updated)
	assert.Equal(t, "Invalid content: []", err.Error())

	// updated
	updated, err = p0.Set(fe, []byte{PieceDirty})
	assert.True(t, updated)
	assert.Nil(t, err)

	// not changed
	updated, err = p0.Set(fe, []byte{PieceDirty})
	assert.False(t, updated)
	assert.Nil(t, err)

	// get
	d0, err := p0.Get(fe)
	assert.Nil(t, err)
	assert.Equal(t, PieceDirty, d0[0])

	d1, err := p1.Get(fe)
	assert.Nil(t, err)
	assert.Equal(t, PieceClean, d1[0])

	content, err := ioutil.ReadFile(fp + "_status")
	assert.Nil(t, err)
	assert.Equal(t, content[0], PieceDirty)
	assert.Equal(t, content[1], PieceClean)

	// set when in cache
	fe.SetState(stateCache)
	updated, err = p0.Set(fe, []byte{PieceDone})
	assert.False(t, updated)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Sprintf("Cannot change piece status for %s: %d. File not in download directory.", fp, 0), err.Error())

	// get when in cache
	d1, err = p1.Get(fe)
	assert.Nil(t, err)
	assert.Equal(t, PieceDone, d1[0])

	fe.SetState(stateTrash)
	d1, err = p1.Get(fe)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Sprintf("Failed to get piece status for %s: %d cannot find file in download nor cache directory.", fp, 1), err.Error())

	fe.SetState(stateDownload)

	// update concurrent
	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		updated, err := p0.Set(fe, []byte{PieceClean})
		wg.Done()
		assert.True(t, updated)
		assert.Nil(t, err)
		// get
		d0, err := p0.Get(fe)
		assert.Nil(t, err)
		assert.Equal(t, PieceClean, d0[0])
	}()

	go func() {
		updated, err := p1.Set(fe, []byte{PieceDone})
		wg.Done()
		assert.True(t, updated)
		assert.Nil(t, err)
		// get
		d1, err := p1.Get(fe)
		assert.Nil(t, err)
		assert.Equal(t, PieceDone, d1[0])
	}()

	wg.Wait()

	content, err = ioutil.ReadFile(fp + "_status")
	assert.Nil(t, err)
	assert.Equal(t, content[0], PieceClean)
	assert.Equal(t, content[1], PieceDone)

	// delete
	assert.Nil(t, p0.Delete(fe))
	assert.Nil(t, p1.Delete(fe))

	_, err = os.Stat(fp + "_status")
	assert.True(t, os.IsNotExist(err))
}

func TestStartedAt(t *testing.T) {
	testDir, err := getTestDir()
	assert.Nil(t, err)
	defer os.RemoveAll(testDir)

	fp := path.Join(testDir, "testStartedAt")
	fe := newMockFileEntry(fp, stateDownload)
	sa := getStartedAt()

	// get on Nil p0
	_, err = sa.Get(fe)
	assert.True(t, os.IsNotExist(err))

	// set and create
	updated, err := sa.Set(fe, nil)
	assert.True(t, updated)
	assert.Nil(t, err)

	// updated
	updated, err = sa.Set(fe, []byte("2017"))
	assert.True(t, updated)
	assert.Nil(t, err)

	// not updated
	updated, err = sa.Set(fe, []byte("2017"))
	assert.False(t, updated)
	assert.Nil(t, err)

	// get
	d, err := sa.Get(fe)
	assert.Nil(t, err)
	assert.Equal(t, "2017", string(d[:]))

	content, err := ioutil.ReadFile(fp + "_startedat")
	assert.Nil(t, err)
	assert.Equal(t, "2017", string(content[:]))

	// delete
	assert.Nil(t, sa.Delete(fe))

	_, err = os.Stat(fp + "_startedat")
	assert.True(t, os.IsNotExist(err))
}

func TestHashState(t *testing.T) {
	testDir, err := getTestDir()
	assert.Nil(t, err)
	defer os.RemoveAll(testDir)

	fp := path.Join(testDir, "testHashState")
	fe := newMockFileEntry(fp, stateDownload)
	hs := getHashState("sha256", "0")

	// get on Nil p0
	_, err = hs.Get(fe)
	assert.True(t, os.IsNotExist(err))

	// set and create
	updated, err := hs.Set(fe, nil)
	assert.True(t, updated)
	assert.Nil(t, err)

	// updated
	updated, err = hs.Set(fe, []byte("2017"))
	assert.True(t, updated)
	assert.Nil(t, err)

	// not updated
	updated, err = hs.Set(fe, []byte("2017"))
	assert.False(t, updated)
	assert.Nil(t, err)

	// get
	d, err := hs.Get(fe)
	assert.Nil(t, err)
	assert.Equal(t, "2017", string(d[:]))

	content, err := ioutil.ReadFile(fp + "_hashstates/sha256_0")
	assert.Nil(t, err)
	assert.Equal(t, "2017", string(content[:]))

	// delete
	assert.Nil(t, hs.Delete(fe))

	_, err = os.Stat(fp + "_hashstates/sha256_0")
	assert.True(t, os.IsNotExist(err))
}
