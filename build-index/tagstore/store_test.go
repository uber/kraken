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
package tagstore_test

import (
	"fmt"
	"io"
	"sync"
	"testing"

	. "github.com/uber/kraken/build-index/tagstore"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/persistedretry/writeback"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/mocks/lib/backend"
	"github.com/uber/kraken/mocks/lib/persistedretry"
	"github.com/uber/kraken/utils/mockutil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const _testNamespace = ".*"

type storeMocks struct {
	ctrl             *gomock.Controller
	ss               *store.SimpleStore
	backends         *backend.Manager
	backendClient    *mockbackend.MockClient
	writeBackManager *mockpersistedretry.MockManager
}

func newStoreMocks(t *testing.T) (*storeMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	ss, c := store.SimpleStoreFixture()
	cleanup.Add(c)

	backends := backend.ManagerFixture()
	backendClient := mockbackend.NewMockClient(ctrl)
	require.NoError(t, backends.Register(_testNamespace, backendClient))

	writeBackManager := mockpersistedretry.NewMockManager(ctrl)

	return &storeMocks{ctrl, ss, backends, backendClient, writeBackManager}, cleanup.Run
}

func (m *storeMocks) new(config Config) Store {
	return New(config, tally.NoopScope, m.ss, m.backends, m.writeBackManager)
}

func checkConcurrentGets(t *testing.T, store Store, tag string, expected core.Digest) {
	t.Helper()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
		}()
	}
	wg.Wait()
}

func TestPutAndGetFromDisk(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(Config{})

	tag := core.TagFixture()
	digest := core.DigestFixture()

	mocks.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(tag, tag, 0))).Return(nil)

	require.NoError(store.Put(tag, digest, 0))

	result, err := store.Get(tag)
	require.NoError(err)
	require.Equal(digest, result)
}

func TestPutAndGetFromDiskWriteThrough(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(Config{WriteThrough: true})

	tag := core.TagFixture()
	digest := core.DigestFixture()

	mocks.writeBackManager.EXPECT().SyncExec(
		writeback.MatchTask(writeback.NewTask(tag, tag, 0))).Return(nil)

	require.NoError(store.Put(tag, digest, 0))

	result, err := store.Get(tag)
	require.NoError(err)
	require.Equal(digest, result)
}

func TestGetFromBackendNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(Config{})

	tag := core.TagFixture()
	digest := core.DigestFixture()

	w := mockutil.MatchWriter([]byte(digest.String()))
	mocks.backendClient.EXPECT().Download(tag, tag, w).Return(backenderrors.ErrBlobNotFound)

	_, err := store.Get(tag)
	require.Error(err)
	require.Equal(ErrTagNotFound, err)
}

func TestGetFromBackendUnkownError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(Config{})

	tag := core.TagFixture()
	digest := core.DigestFixture()

	w := mockutil.MatchWriter([]byte(digest.String()))
	mocks.backendClient.EXPECT().Download(tag, tag, w).Return(fmt.Errorf("test error"))

	_, err := store.Get(tag)
	require.Error(err)
}

func TestGetFromBackendInvalidValue(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(Config{})

	tag := core.TagFixture()
	digest := core.DigestFixture()

	mocks.backendClient.EXPECT().Download(
		tag, tag,
		mockutil.MatchWriter([]byte(digest.String()))).DoAndReturn(
		func(namespace, name string, dst io.Writer) error {
			dst.Write([]byte("foo"))
			return nil
		})

	_, err := store.Get(tag)
	require.Error(err)
}
