// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package transfer

import (
	"errors"
	"io"
	"testing"

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/disk"
	mocktagclient "github.com/uber/kraken/mocks/build-index/tagclient"
	mockblobclient "github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/mockutil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

type proxyTransfererMocks struct {
	tags          *mocktagclient.MockClient
	originCluster *mockblobclient.MockClusterClient
	store         *disk.Store
}

func newReadWriteTransfererMocks(t *testing.T) (*proxyTransfererMocks, func()) {
	var cleanup testutil.Cleanup

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	tags := mocktagclient.NewMockClient(ctrl)

	originCluster := mockblobclient.NewMockClusterClient(ctrl)

	store := disk.Fixture(t)

	return &proxyTransfererMocks{tags, originCluster, store}, cleanup.Run
}

func (m *proxyTransfererMocks) new() *ReadWriteTransferer {
	return NewReadWriteTransferer(tally.NoopScope, m.tags, m.originCluster, m.store)
}

func TestReadWriteTransfererDownloadCachesBlob(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadWriteTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/test-image"
	blob := core.NewBlobFixture()

	mocks.originCluster.EXPECT().DownloadBlob(
		gomock.Any(), namespace, blob.Digest, mockutil.MatchWriter(blob.Content)).Return(nil)

	// Downloading multiple times should only call blob download once.
	for i := 0; i < 10; i++ {
		result, err := transferer.Download(namespace, blob.Digest)
		require.NoError(err)
		b, err := io.ReadAll(result)
		require.NoError(err)
		require.Equal(blob.Content, b)
	}
}

func TestReadWriteTransfererGetTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadWriteTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	tag := "docker/some-tag"
	manifest := core.DigestFixture()

	mocks.tags.EXPECT().Get(tag).Return(manifest, nil)

	d, err := transferer.GetTag(tag)
	require.NoError(err)
	require.Equal(manifest, d)
}

func TestReadWriteTransfererGetTagNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadWriteTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	tag := "docker/some-tag"

	mocks.tags.EXPECT().Get(tag).Return(core.Digest{}, tagclient.ErrTagNotFound)

	_, err := transferer.GetTag(tag)
	require.Error(err)
	require.Equal(ErrTagNotFound, err)
}

func TestReadWriteTransfererPutTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadWriteTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	config := core.DigestFixture()
	layer1 := core.DigestFixture()
	layer2 := core.DigestFixture()

	manifestDigest, rawManifest := dockerutil.ManifestFixture(config, layer1, layer2)

	disk.RunDownload(t, mocks.store, manifestDigest.Hex(), rawManifest)

	tag := "docker/some-tag"

	mocks.tags.EXPECT().PutAndReplicate(tag, manifestDigest).Return(nil)

	require.NoError(transferer.PutTag(tag, manifestDigest))
}

func TestReadWriteTransfererStatLocalBlob(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadWriteTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/test-image"
	blob := core.NewBlobFixture()

	disk.RunDownload(t, mocks.store, blob.Digest.Hex(), blob.Content)

	bi, err := transferer.Stat(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(blob.Info(), bi)
}

func TestReadWriteTransfererStatRemoteBlob(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadWriteTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/test-image"
	blob := core.NewBlobFixture()

	mocks.originCluster.EXPECT().Stat(namespace, blob.Digest).Return(blob.Info(), nil)

	bi, err := transferer.Stat(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(blob.Info(), bi)
}

func TestReadWriteTransfererStatNotFoundOnAnyOriginError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadWriteTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/test-image"
	blob := core.NewBlobFixture()

	mocks.originCluster.EXPECT().Stat(namespace, blob.Digest).Return(nil, errors.New("any error"))

	_, err := transferer.Stat(namespace, blob.Digest)
	require.Equal(ErrBlobNotFound, err)
}
