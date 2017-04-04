package store

import (
	"io/ioutil"
	"sync"
	"testing"

	"path"

	"os"

	"github.com/stretchr/testify/assert"
)

func getTestDir() (string, error) {
	return ioutil.TempDir("./.tmp/", "metadata")
}

func TestPieceStatus(t *testing.T) {
	testDir, err := getTestDir()
	assert.Nil(t, err)
	defer os.RemoveAll(testDir)

	fp := path.Join(testDir, "testPieceStatus")
	p0 := getPieceStatus(0, 2)
	p1 := getPieceStatus(1, 2)

	// get on Nil p0
	_, err = p0.Get(fp)
	assert.True(t, os.IsNotExist(err))

	// set on Nil content p0
	updated, err := p0.Set(fp, nil)
	assert.False(t, updated)
	assert.Equal(t, "Invalid content: []", err.Error())

	// updated
	updated, err = p0.Set(fp, []byte{pieceDone})
	assert.True(t, updated)
	assert.Nil(t, err)

	// not changed
	updated, err = p0.Set(fp, []byte{pieceDone})
	assert.False(t, updated)
	assert.Nil(t, err)

	content, err := ioutil.ReadFile(fp + "_status")
	assert.Nil(t, err)
	assert.Equal(t, content[0], pieceDone)
	assert.Equal(t, content[1], pieceClean)

	// update concurrent
	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		updated, err := p0.Set(fp, []byte{pieceDirty})
		wg.Done()
		assert.True(t, updated)
		assert.Nil(t, err)
		// get
		d0, err := p0.Get(fp)
		assert.Nil(t, err)
		assert.Equal(t, pieceDirty, d0[0])
	}()

	go func() {
		updated, err := p1.Set(fp, []byte{pieceDone})
		wg.Done()
		assert.True(t, updated)
		assert.Nil(t, err)
		// get
		d1, err := p1.Get(fp)
		assert.Nil(t, err)
		assert.Equal(t, d1[0], pieceDone)
	}()

	wg.Wait()

	content, err = ioutil.ReadFile(fp + "_status")
	assert.Nil(t, err)
	assert.Equal(t, content[0], pieceDirty)
	assert.Equal(t, content[1], pieceDone)

	// delete
	assert.Nil(t, p0.Delete(fp))
	assert.Nil(t, p1.Delete(fp))

	_, err = os.Stat(fp + "_status")
	assert.True(t, os.IsNotExist(err))
}

func TestStartedAt(t *testing.T) {
	testDir, err := getTestDir()
	assert.Nil(t, err)
	defer os.RemoveAll(testDir)

	fp := path.Join(testDir, "testStartedAt")
	sa := getStartedAt()

	// get on Nil p0
	_, err = sa.Get(fp)
	assert.True(t, os.IsNotExist(err))

	// set and create
	updated, err := sa.Set(fp, nil)
	assert.True(t, updated)
	assert.Nil(t, err)

	// updated
	updated, err = sa.Set(fp, []byte("2017"))
	assert.True(t, updated)
	assert.Nil(t, err)

	// not updated
	updated, err = sa.Set(fp, []byte("2017"))
	assert.False(t, updated)
	assert.Nil(t, err)

	// get
	d, err := sa.Get(fp)
	assert.Nil(t, err)
	assert.Equal(t, "2017", string(d[:]))

	content, err := ioutil.ReadFile(fp + "_startedat")
	assert.Nil(t, err)
	assert.Equal(t, "2017", string(content[:]))

	// delete
	assert.Nil(t, sa.Delete(fp))

	_, err = os.Stat(fp + "_startedat")
	assert.True(t, os.IsNotExist(err))
}

func TestHashState(t *testing.T) {
	testDir, err := getTestDir()
	assert.Nil(t, err)
	defer os.RemoveAll(testDir)

	fp := path.Join(testDir, "testHashState")
	hs := getHashState("sha256", "0")

	// get on Nil p0
	_, err = hs.Get(fp)
	assert.True(t, os.IsNotExist(err))

	// set and create
	updated, err := hs.Set(fp, nil)
	assert.True(t, updated)
	assert.Nil(t, err)

	// updated
	updated, err = hs.Set(fp, []byte("2017"))
	assert.True(t, updated)
	assert.Nil(t, err)

	// not updated
	updated, err = hs.Set(fp, []byte("2017"))
	assert.False(t, updated)
	assert.Nil(t, err)

	// get
	d, err := hs.Get(fp)
	assert.Nil(t, err)
	assert.Equal(t, "2017", string(d[:]))

	content, err := ioutil.ReadFile(fp + "_hashstates/sha256_0")
	assert.Nil(t, err)
	assert.Equal(t, "2017", string(content[:]))

	// delete
	assert.Nil(t, hs.Delete(fp))

	_, err = os.Stat(fp + "_hashstates/sha256_0")
	assert.True(t, os.IsNotExist(err))
}

func TestCompareMetadata(t *testing.T) {
	d1 := []byte("2017")
	d2 := []byte("2018")
	d3 := []byte("201")
	d4 := []byte("2018")
	assert.False(t, compareMetadata(d1, d2))
	assert.False(t, compareMetadata(d1, d3))
	assert.True(t, compareMetadata(d2, d4))
}
