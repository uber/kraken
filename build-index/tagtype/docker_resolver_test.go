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
package tagtype

import (
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	mockblobclient "github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/mockutil"
)

func newTestDockerResolver(ctrl *gomock.Controller) (*dockerResolver, *mockblobclient.MockClusterClient) {
	originClient := mockblobclient.NewMockClusterClient(ctrl)

	// Use minimal backoff for fast tests
	backoffConfig := httputil.ExponentialBackOffConfig{
		Enabled:             true,
		InitialInterval:     1 * time.Millisecond,
		RandomizationFactor: 0,
		Multiplier:          2,
		MaxInterval:         10 * time.Millisecond,
		MaxRetries:          3,
	}

	resolver := &dockerResolver{
		originClient:  originClient,
		backoffConfig: backoffConfig,
	}

	return resolver, originClient
}

// setupDockerResolverTest sets up common test dependencies.
// Returns require, ctrl, resolver, and mockOrigin.
func setupDockerResolverTest(t *testing.T) (*require.Assertions, *gomock.Controller, *dockerResolver, *mockblobclient.MockClusterClient) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	resolver, mockOrigin := newTestDockerResolver(ctrl)

	return require, ctrl, resolver, mockOrigin
}

// setupDockerResolverTestWithManifest sets up common test dependencies with manifest fixtures.
// Returns require, ctrl, resolver, mockOrigin, tag, layers, manifest digest, and manifest bytes.
func setupDockerResolverTestWithManifest(t *testing.T) (*require.Assertions, *gomock.Controller, *dockerResolver, *mockblobclient.MockClusterClient, string, core.DigestList, core.Digest, []byte) {
	require, ctrl, resolver, mockOrigin := setupDockerResolverTest(t)

	tag := "repo/image:v1.0"
	layers := core.DigestListFixture(3)
	manifest, manifestBytes := dockerutil.ManifestFixture(layers[0], layers[1], layers[2])

	return require, ctrl, resolver, mockOrigin, tag, layers, manifest, manifestBytes
}

func TestDockerResolver_DownloadManifest_Success(t *testing.T) {
	require, _, resolver, mockOrigin, tag, _, manifest, manifestBytes := setupDockerResolverTestWithManifest(t)

	// Expect successful download on first attempt
	mockOrigin.EXPECT().
		DownloadBlob(tag, manifest, mockutil.MatchWriter(manifestBytes)).
		Return(nil)

	result, err := resolver.downloadManifest(tag, manifest)
	require.NoError(err)
	require.NotNil(result)
}

func TestDockerResolver_DownloadManifest_RetryOnBlobNotFound(t *testing.T) {
	require, _, resolver, mockOrigin, tag, _, manifest, manifestBytes := setupDockerResolverTestWithManifest(t)

	// First attempt fails with blob not found, second succeeds
	gomock.InOrder(
		mockOrigin.EXPECT().
			DownloadBlob(tag, manifest, gomock.Any()).
			Return(blobclient.ErrBlobNotFound),
		mockOrigin.EXPECT().
			DownloadBlob(tag, manifest, mockutil.MatchWriter(manifestBytes)).
			Return(nil),
	)

	result, err := resolver.downloadManifest(tag, manifest)
	require.NoError(err)
	require.NotNil(result)
}

func TestDockerResolver_DownloadManifest_ExhaustedRetries(t *testing.T) {
	require, _, resolver, mockOrigin := setupDockerResolverTest(t)

	tag := "repo/image:v1.0"
	manifest := core.DigestFixture()

	// All attempts fail with blob not found (MaxRetries=3, so 4 total attempts)
	mockOrigin.EXPECT().
		DownloadBlob(tag, manifest, gomock.Any()).
		Return(blobclient.ErrBlobNotFound).
		Times(4)

	result, err := resolver.downloadManifest(tag, manifest)
	require.Error(err)
	require.Nil(result)
	require.Equal(blobclient.ErrBlobNotFound, err)
}

func TestDockerResolver_DownloadManifest_PermanentError(t *testing.T) {
	require, _, resolver, mockOrigin := setupDockerResolverTest(t)

	tag := "repo/image:v1.0"
	manifest := core.DigestFixture()

	// 401 Unauthorized is a permanent error (4xx except 404)
	permanentErr := httputil.StatusError{Status: 401}

	mockOrigin.EXPECT().
		DownloadBlob(tag, manifest, gomock.Any()).
		Return(permanentErr).
		Times(1) // Should only be called once, no retries

	result, err := resolver.downloadManifest(tag, manifest)
	require.Error(err)
	require.Nil(result)
}

func TestDockerResolver_DownloadManifest_BufferResetBetweenRetries(t *testing.T) {
	require, _, resolver, mockOrigin, tag, _, manifest, manifestBytes := setupDockerResolverTestWithManifest(t)

	partialData := []byte("partial corrupt data")

	// First attempt writes partial data then fails
	// Second attempt succeeds with full data
	gomock.InOrder(
		mockOrigin.EXPECT().
			DownloadBlob(tag, manifest, gomock.Any()).
			DoAndReturn(func(tag string, d core.Digest, dst io.Writer) error {
				_, err := dst.Write(partialData)
				require.NoError(err)
				return blobclient.ErrBlobNotFound
			}),
		mockOrigin.EXPECT().
			DownloadBlob(tag, manifest, mockutil.MatchWriter(manifestBytes)).
			Return(nil),
	)

	result, err := resolver.downloadManifest(tag, manifest)
	require.NoError(err)
	require.NotNil(result)
	// If buffer wasn't reset, parsing would fail due to partial data
}

func TestDockerResolver_DownloadManifest_InvalidManifestFormat(t *testing.T) {
	require, _, resolver, mockOrigin := setupDockerResolverTest(t)

	tag := "repo/image:v1.0"
	manifest := core.DigestFixture()

	// Download succeeds but returns invalid manifest data
	mockOrigin.EXPECT().
		DownloadBlob(tag, manifest, gomock.Any()).
		DoAndReturn(func(tag string, d core.Digest, dst io.Writer) error {
			_, err := dst.Write([]byte("invalid manifest json"))
			require.NoError(err)
			return nil
		})

	result, err := resolver.downloadManifest(tag, manifest)
	require.Error(err)
	require.Nil(result)
	require.Contains(err.Error(), "parse manifest")
}

func TestDockerResolver_Resolve_Success(t *testing.T) {
	require, _, resolver, mockOrigin, tag, layers, manifest, manifestBytes := setupDockerResolverTestWithManifest(t)

	mockOrigin.EXPECT().
		DownloadBlob(tag, manifest, mockutil.MatchWriter(manifestBytes)).
		Return(nil)

	deps, err := resolver.Resolve(tag, manifest)
	require.NoError(err)
	require.Equal(core.DigestList(append(layers, manifest)), deps)
}

func TestDockerResolver_Resolve_DownloadError(t *testing.T) {
	require, _, resolver, mockOrigin := setupDockerResolverTest(t)

	tag := "repo/image:v1.0"
	manifest := core.DigestFixture()

	// All retries exhausted
	mockOrigin.EXPECT().
		DownloadBlob(tag, manifest, gomock.Any()).
		Return(blobclient.ErrBlobNotFound).
		Times(4)

	deps, err := resolver.Resolve(tag, manifest)
	require.Error(err)
	require.Nil(deps)
	require.Contains(err.Error(), "download manifest")
}

func TestDockerResolver_Resolve_WithRetries(t *testing.T) {
	require, _, resolver, mockOrigin, tag, layers, manifest, manifestBytes := setupDockerResolverTestWithManifest(t)

	// Fails twice, succeeds on third attempt
	gomock.InOrder(
		mockOrigin.EXPECT().
			DownloadBlob(tag, manifest, gomock.Any()).
			Return(blobclient.ErrBlobNotFound),
		mockOrigin.EXPECT().
			DownloadBlob(tag, manifest, gomock.Any()).
			Return(blobclient.ErrBlobNotFound),
		mockOrigin.EXPECT().
			DownloadBlob(tag, manifest, mockutil.MatchWriter(manifestBytes)).
			Return(nil),
	)

	deps, err := resolver.Resolve(tag, manifest)
	require.NoError(err)
	require.NotNil(deps)
	require.Equal(core.DigestList(append(layers, manifest)), deps)
}
