package internal

import (
	"io/ioutil"
	"log"
	"os"

	"code.uber.internal/infra/kraken/utils/testutil"

	"golang.org/x/sync/syncmap"
)

const (
	_testFileName = "test_file"
)

type mockFileState struct {
	dir string
}

func (state mockFileState) GetDirectory() string { return state.dir }

func fileStatesFixture() (state1, state2, state3 mockFileState, run func()) {
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	root, err := ioutil.TempDir("/tmp", "store_test")
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

type fileEntryTestBundle struct {
	state1 mockFileState
	state2 mockFileState
	state3 mockFileState

	entry FileEntry
}

func fileEntryLocalFixture() (bundle *fileEntryTestBundle, run func()) {
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	state1, state2, state3, f := fileStatesFixture()
	cleanup.Add(f)
	entry := NewLocalFileEntryFactory().Create(_testFileName, state1)

	return &fileEntryTestBundle{
		state1: state1,
		state2: state2,
		state3: state3,
		entry:  entry,
	}, cleanup.Run
}

type fileMapTestBundle struct {
	state1 mockFileState
	state2 mockFileState
	state3 mockFileState

	entry FileEntry
	fm    FileMap
}

func fileMapSimpleFixture() (bundle *fileMapTestBundle, run func()) {
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	b, clean := fileEntryLocalFixture()
	cleanup.Add(clean)

	fm := &simpleFileMap{m: syncmap.Map{}}

	return &fileMapTestBundle{
		state1: b.state1,
		state2: b.state2,
		state3: b.state3,
		entry:  b.entry,
		fm:     fm,
	}, cleanup.Run
}

func fileMapLRUFixture() (bundle *fileMapTestBundle, run func()) {
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	b, clean := fileEntryLocalFixture()
	cleanup.Add(clean)

	fm, err := NewLRUFileMap(100)
	if err != nil {
		log.Fatal(err)
	}

	return &fileMapTestBundle{
		state1: b.state1,
		state2: b.state2,
		state3: b.state3,
		entry:  b.entry,
		fm:     fm,
	}, cleanup.Run
}

// fileStoreBundle contains available states, FileStore and a map of FileEntry
// NOTE: do not use this struct directly, use fixtures instead
// TODO: breakdown fileStoreTestBundle
type fileStoreTestBundle struct {
	state1 mockFileState
	state2 mockFileState
	state3 mockFileState

	createStore func() *localFileStore
	store       *localFileStore
	files       map[mockFileState]string
}

func (b *fileStoreTestBundle) recreateStore() {
	b.store = b.createStore()
}

func fileStoreDefaultFixture() (*fileStoreTestBundle, func()) {
	return newFileStoreFixture(func() *localFileStore {
		store, err := NewLocalFileStore()
		if err != nil {
			log.Fatal(err)
		}
		return store.(*localFileStore)
	})
}

func fileStoreCASFixture() (*fileStoreTestBundle, func()) {
	return newFileStoreFixture(func() *localFileStore {
		store, err := NewCASFileStore()
		if err != nil {
			log.Fatal(err)
		}
		return store.(*localFileStore)
	})
}

func fileStoreLRUFixture(size int) (*fileStoreTestBundle, func()) {
	return newFileStoreFixture(func() *localFileStore {
		store, err := NewLRUFileStore(size)
		if err != nil {
			log.Fatal(err)
		}
		return store.(*localFileStore)
	})
}

func newFileStoreFixture(createStore func() *localFileStore) (*fileStoreTestBundle, func()) {
	store := createStore()
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	state1, state2, state3, f := fileStatesFixture()
	cleanup.Add(f)

	storeBundle := &fileStoreTestBundle{
		state1:      state1,
		state2:      state2,
		state3:      state3,
		createStore: createStore,
		store:       store,
		files:       make(map[mockFileState]string),
	}

	// Create one test file in store
	err := storeBundle.store.NewFileOp().CreateFile(_testFileName, storeBundle.state1, 5)
	if err != nil {
		log.Fatal(err)
	}

	storeBundle.files[storeBundle.state1] = _testFileName

	return storeBundle, cleanup.Run
}

type rcFileStoreTestBundle struct {
	state1 mockFileState
	state2 mockFileState
	state3 mockFileState

	createStore func() *localRCFileStore
	store       *localRCFileStore
	files       map[mockFileState]string
}

func rcFileStoreFixture() (*rcFileStoreTestBundle, func()) {
	return newRCFileStoreFixture(func() *localRCFileStore {
		store, err := NewLocalRCFileStore()
		if err != nil {
			log.Fatal(err)
		}
		return store.(*localRCFileStore)
	})
}

func newRCFileStoreFixture(createStore func() *localRCFileStore) (*rcFileStoreTestBundle, func()) {
	store := createStore()
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	state1, state2, state3, f := fileStatesFixture()
	cleanup.Add(f)

	storeBundle := &rcFileStoreTestBundle{
		state1:      state1,
		state2:      state2,
		state3:      state3,
		createStore: createStore,
		store:       store,
		files:       make(map[mockFileState]string),
	}

	// Create one test file in store
	err := storeBundle.store.NewFileOp().CreateFile(_testFileName, storeBundle.state1, 5)
	if err != nil {
		log.Fatal(err)
	}

	storeBundle.files[storeBundle.state1] = _testFileName

	return storeBundle, cleanup.Run
}
