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
package writeback

import (
	"bytes"
	"errors"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/mocks/lib/backend"
	"github.com/uber/kraken/utils/mockutil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

type executorMocks struct {
	ctrl     *gomock.Controller
	cas      *store.CAStore
	backends *backend.Manager
}

func newExecutorMocks(t *testing.T) (*executorMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	cas, c := store.CAStoreFixture()
	cleanup.Add(c)

	return &executorMocks{
		ctrl:     ctrl,
		cas:      cas,
		backends: backend.ManagerFixture(),
	}, cleanup.Run
}

func (m *executorMocks) new() *Executor {
	return NewExecutor(tally.NoopScope, m.cas, m.backends)
}

func (m *executorMocks) client(namespace string) *mockbackend.MockClient {
	client := mockbackend.NewMockClient(m.ctrl)
	if err := m.backends.Register(namespace, client); err != nil {
		panic(err)
	}
	return client
}

func setupBlob(t *testing.T, cas *store.CAStore, blob *core.BlobFixture) {
	t.Helper()
	require.NoError(t, cas.CreateCacheFile(blob.Digest.Hex(), bytes.NewReader(blob.Content)))
	_, err := cas.SetCacheFileMetadata(blob.Digest.Hex(), metadata.NewPersist(true))
	require.NoError(t, err)
}

func TestExec(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	blob := core.NewBlobFixture()

	setupBlob(t, mocks.cas, blob)

	task := NewTask(core.TagFixture(), blob.Digest.Hex(), 0)

	client := mocks.client(task.Namespace)
	client.EXPECT().Stat(task.Namespace, blob.Digest.Hex()).Return(nil, backenderrors.ErrBlobNotFound)
	client.EXPECT().Upload(task.Namespace, blob.Digest.Hex(), mockutil.MatchReader(blob.Content)).Return(nil)

	executor := mocks.new()

	require.NoError(executor.Exec(task))

	// Should be safe to delete the file.
	require.NoError(mocks.cas.DeleteCacheFile(blob.Digest.Hex()))
}

func TestExecNoopWhenFileAlreadyUploaded(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	blob := core.NewBlobFixture()

	setupBlob(t, mocks.cas, blob)

	require.NoError(mocks.cas.CreateCacheFile(blob.Digest.Hex(), bytes.NewReader(blob.Content)))

	task := NewTask(core.TagFixture(), blob.Digest.Hex(), 0)

	client := mocks.client(task.Namespace)
	client.EXPECT().Stat(task.Namespace, blob.Digest.Hex()).Return(core.NewBlobInfo(blob.Length()), nil)

	executor := mocks.new()

	require.NoError(executor.Exec(task))

	// Should be safe to delete the file.
	require.NoError(mocks.cas.DeleteCacheFile(blob.Digest.Hex()))
}

func TestExecNoopWhenFileMissing(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	blob := core.NewBlobFixture()

	task := NewTask(core.TagFixture(), blob.Digest.Hex(), 0)

	client := mocks.client(task.Namespace)
	client.EXPECT().Stat(task.Namespace, blob.Digest.Hex()).Return(nil, backenderrors.ErrBlobNotFound)

	executor := mocks.new()

	require.NoError(executor.Exec(task))
}

func TestExecNoopWhenNamespaceNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	blob := core.NewBlobFixture()

	setupBlob(t, mocks.cas, blob)

	require.NoError(mocks.cas.CreateCacheFile(blob.Digest.Hex(), bytes.NewReader(blob.Content)))

	task := NewTask(core.TagFixture(), blob.Digest.Hex(), 0)

	executor := mocks.new()

	require.NoError(executor.Exec(task))

	// Should be safe to delete the file.
	require.NoError(mocks.cas.DeleteCacheFile(blob.Digest.Hex()))
}

func TestExecUploadFailure(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	blob := core.NewBlobFixture()

	setupBlob(t, mocks.cas, blob)

	task := NewTask(core.TagFixture(), blob.Digest.Hex(), 0)

	client := mocks.client(task.Namespace)
	client.EXPECT().Stat(task.Namespace, blob.Digest.Hex()).Return(nil, backenderrors.ErrBlobNotFound)
	client.EXPECT().Upload(task.Namespace,
		blob.Digest.Hex(), mockutil.MatchReader(blob.Content)).Return(errors.New("some error"))

	executor := mocks.new()

	require.Error(executor.Exec(task))

	// Since upload failed, deletion of the file should fail since persist
	// metadata is still present.
	require.Error(mocks.cas.DeleteCacheFile(blob.Digest.Hex()))
}
