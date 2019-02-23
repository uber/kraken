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
package tagtype

import (
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/mockutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func testConfigs() []Config {
	return []Config{
		{Namespace: "namespace-foo/.*", Type: "docker"},
		{Namespace: "namespace-bar/.*", Type: "default"},
	}
}

func TestMapResolveDocker(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originClient := mockblobclient.NewMockClusterClient(ctrl)

	m, err := NewMap(testConfigs(), originClient)
	require.NoError(err)

	tag := "namespace-foo/repo-bar:0001"
	layers := core.DigestListFixture(3)
	manifest, b := dockerutil.ManifestFixture(layers[0], layers[1], layers[2])

	originClient.EXPECT().DownloadBlob(tag, manifest, mockutil.MatchWriter(b)).Return(nil)

	deps, err := m.Resolve(tag, manifest)
	require.NoError(err)
	require.Equal(core.DigestList(append(layers, manifest)), deps)
}

func TestMapResolveDefault(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originClient := mockblobclient.NewMockClusterClient(ctrl)

	m, err := NewMap(testConfigs(), originClient)
	require.NoError(err)

	tag := "namespace-bar/repo-bar:0001"
	d := core.DigestFixture()

	deps, err := m.Resolve(tag, d)
	require.NoError(err)
	require.Equal(core.DigestList{d}, deps)
}

func TestMapResolveUndefined(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originClient := mockblobclient.NewMockClusterClient(ctrl)

	conf := []Config{
		{Namespace: "namespace-hello/.*", Type: "undefined"},
	}
	_, err := NewMap(conf, originClient)
	require.Error(err)
}

func TestMapGetResolverNamespaceError(t *testing.T) {
	require := require.New(t)

	m, err := NewMap(testConfigs(), nil)
	require.NoError(err)

	_, err = m.Resolve("invalid/tag", core.DigestFixture())
	require.Error(err)
	require.Equal(errNamespaceNotFound, err)
}
