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
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/testutil"

	"github.com/andres-erbsen/clock"
)

func fileStatesFixture() (state1, state2, state3 FileState, run func()) {
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

	state1 = NewFileState(state1Dir)
	state2 = NewFileState(state2Dir)
	state3 = NewFileState(state3Dir)

	return state1, state2, state3, cleanup.Run
}

type fileEntryTestBundle struct {
	state1 FileState
	state2 FileState
	state3 FileState

	entry FileEntry
}

func fileEntryLocalFixture() (bundle *fileEntryTestBundle, run func()) {
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	state1, state2, state3, f := fileStatesFixture()
	cleanup.Add(f)
	entry, err := NewLocalFileEntryFactory().Create(core.DigestFixture().Hex(), state1)
	if err != nil {
		panic(fmt.Sprintf("create test file: %s", err))
	}

	return &fileEntryTestBundle{
		state1: state1,
		state2: state2,
		state3: state3,
		entry:  entry,
	}, cleanup.Run
}

type fileMapTestBundle struct {
	state1 FileState
	state2 FileState
	state3 FileState

	entry FileEntry
	fm    FileMap
}

func fileMapLRUFixture() (bundle *fileMapTestBundle, run func()) {
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	b, clean := fileEntryLocalFixture()
	cleanup.Add(clean)

	fm := NewLRUFileMap(100, clock.New())

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
	clk clock.Clock

	state1 FileState
	state2 FileState
	state3 FileState

	createStore func(clk clock.Clock) *localFileStore
	store       *localFileStore
	files       map[FileState]string
}

func (b *fileStoreTestBundle) recreateStore() {
	b.store = b.createStore(b.clk)
}

func fileStoreDefaultFixture() (*fileStoreTestBundle, func()) {
	return fileStoreFixture(func(clk clock.Clock) *localFileStore {
		store := NewLocalFileStore(clk)
		return store.(*localFileStore)
	})
}

func fileStoreCASFixture() (*fileStoreTestBundle, func()) {
	return fileStoreFixture(func(clk clock.Clock) *localFileStore {
		store := NewCASFileStore(clk)
		return store.(*localFileStore)
	})
}

func fileStoreLRUFixture(size int) (*fileStoreTestBundle, func()) {
	return fileStoreFixture(func(clk clock.Clock) *localFileStore {
		store := NewLRUFileStore(size, clk)
		return store.(*localFileStore)
	})
}

func fileStoreFixture(
	createStore func(clk clock.Clock) *localFileStore) (*fileStoreTestBundle, func()) {

	clk := clock.NewMock()
	store := createStore(clk)
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	state1, state2, state3, f := fileStatesFixture()
	cleanup.Add(f)

	storeBundle := &fileStoreTestBundle{
		clk:         clk,
		state1:      state1,
		state2:      state2,
		state3:      state3,
		createStore: createStore,
		store:       store,
		files:       make(map[FileState]string),
	}

	// Create one test file in store
	testFile := core.DigestFixture().Hex()
	err := storeBundle.store.NewFileOp().CreateFile(testFile, storeBundle.state1, 5)
	if err != nil {
		log.Fatal(err)
	}

	storeBundle.files[storeBundle.state1] = testFile

	return storeBundle, cleanup.Run
}
