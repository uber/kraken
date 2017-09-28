package base

import (
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"

	"code.uber.internal/infra/kraken/testutils"
)

const (
	_testFileName = "test_file.txt"
)

// Mock metadata
func init() {
	RegisterMetadata(regexp.MustCompile("_mocksuffix_\\w+"), &mockMetadataFactory{})
	RegisterMetadata(regexp.MustCompile("_mocksuffix_movable"), &mockMetadataFactoryMovable{})
}

type mockFileState struct {
	dir string
}

func (state mockFileState) GetDirectory() string { return state.dir }

func fileStatesFixture() (state1, state2, state3 mockFileState, run func()) {
	cleanup := &testutils.Cleanup{}
	defer cleanup.Recover()

	root, err := ioutil.TempDir("/tmp", "store")
	if err != nil {
		log.Fatal(err)
	}
	cleanup.Add(func() { os.RemoveAll(root) })

	state1Dir, err := ioutil.TempDir(root, "state1")
	if err != nil {
		log.Fatal(err)
	}

	state2Dir, err := ioutil.TempDir(root, "state2")
	if err != nil {
		log.Fatal(err)
	}

	state3Dir, err := ioutil.TempDir(root, "state3")
	if err != nil {
		log.Fatal(err)
	}

	state1 = mockFileState{state1Dir}
	state2 = mockFileState{state2Dir}
	state3 = mockFileState{state3Dir}

	return state1, state2, state3, cleanup.Run
}

type mockMetadataFactory struct{}

func (f mockMetadataFactory) Create(suffix string) MetadataType {
	if strings.HasSuffix(suffix, getMockMetadataOne().GetSuffix()) {
		return getMockMetadataOne()
	}
	if strings.HasSuffix(suffix, getMockMetadataTwo().GetSuffix()) {
		return getMockMetadataTwo()
	}
	return nil
}

type mockMetadata struct {
	randomSuffix string
}

func getMockMetadataOne() MetadataType {
	return mockMetadata{
		randomSuffix: "_mocksuffix_one",
	}
}

func getMockMetadataTwo() MetadataType {
	return mockMetadata{
		randomSuffix: "_mocksuffix_two",
	}
}

func (m mockMetadata) GetSuffix() string {
	return m.randomSuffix
}

func (m mockMetadata) Movable() bool {
	return false
}

type mockMetadataFactoryMovable struct{}

func (f mockMetadataFactoryMovable) Create(suffix string) MetadataType {
	if strings.HasSuffix(suffix, getMockMetadataMovable().GetSuffix()) {
		return getMockMetadataMovable()
	}
	return nil
}

type mockMetadataMovable struct {
	randomSuffix string
}

func getMockMetadataMovable() MetadataType {
	return mockMetadataMovable{
		randomSuffix: "_mocksuffix_movable",
	}
}

func (m mockMetadataMovable) GetSuffix() string {
	return m.randomSuffix
}

func (m mockMetadataMovable) Movable() bool {
	return true
}

type fileEntryTestBundle struct {
	name   string
	verify func(entry FileEntry) error

	entry FileEntry
}

// fileStoreBundle contains available states, FileStore and a map of FileEntry
type fileStoreTestBundle struct {
	state1 mockFileState
	state2 mockFileState
	state3 mockFileState

	store FileStore
	files map[mockFileState]*fileEntryTestBundle
}

func fileStoreLRUFixture(size int) (*fileStoreTestBundle, func()) {
	store, err := NewLocalFileStoreWithLRU(size)
	if err != nil {
		log.Fatal(err)
	}

	return newFileStoreFixture(store)
}

func fileStoreShardDefaultFixture() (*fileStoreTestBundle, func()) {
	store, err := NewShardedFileStoreDefault()
	if err != nil {
		log.Fatal(err)
	}

	return newFileStoreFixture(store)
}

func fileStoreDefaultFixture() (*fileStoreTestBundle, func()) {
	store, err := NewLocalFileStoreDefault()
	if err != nil {
		log.Fatal(err)
	}

	return newFileStoreFixture(store)
}

func newFileStoreFixture(store FileStore) (*fileStoreTestBundle, func()) {
	cleanup := &testutils.Cleanup{}
	defer cleanup.Recover()

	state1, state2, state3, f := fileStatesFixture()
	cleanup.Add(f)

	storeBundle := &fileStoreTestBundle{
		state1: state1,
		state2: state2,
		state3: state3,
		store:  store,
		files:  make(map[mockFileState]*fileEntryTestBundle),
	}

	// Create one test file in store
	err := storeBundle.store.CreateFile(_testFileName, []FileState{}, storeBundle.state1, 5)
	if err != nil {
		log.Fatal(err)
	}

	entry, _, err := storeBundle.store.(*LocalFileStore).LoadFileEntry(_testFileName, []FileState{storeBundle.state1})
	if err != nil {
		log.Fatal(err)
	}

	fileBundle := &fileEntryTestBundle{
		name:   _testFileName,
		verify: func(FileEntry) error { return nil },
		entry:  entry,
	}
	storeBundle.files[storeBundle.state1] = fileBundle

	return storeBundle, cleanup.Run
}
