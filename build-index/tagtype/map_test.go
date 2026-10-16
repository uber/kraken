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
	"testing"
	"time"

	"github.com/uber/kraken/core"
	mockblobclient "github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/mockutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
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

	originClient.EXPECT().DownloadBlob(gomock.Any(), tag, manifest, mockutil.MatchWriter(b)).Return(nil)

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

func TestMapDockerBackoffConfig(t *testing.T) {
	defaults := httputil.ExponentialBackOffConfig{
		Enabled:             true,
		InitialInterval:     500 * time.Millisecond,
		RandomizationFactor: 0.05,
		Multiplier:          2,
		MaxInterval:         30 * time.Second,
		MaxRetries:          3,
	}
	partial := defaults
	partial.InitialInterval = 2 * time.Second
	partial.MaxRetries = 6

	tests := []struct {
		desc   string
		config string
		want   httputil.ExponentialBackOffConfig
	}{
		{
			desc: "no backoff configured",
			config: `
- namespace: .*
  type: docker
`,
			want: defaults,
		},
		{
			desc: "every field configured",
			config: `
- namespace: .*
  type: docker
  backoff:
    enabled: true
    initial_interval: 1s
    randomization_factor: 0.1
    multiplier: 3
    max_interval: 1m
    max_retries: 5
`,
			want: httputil.ExponentialBackOffConfig{
				Enabled:             true,
				InitialInterval:     time.Second,
				RandomizationFactor: 0.1,
				Multiplier:          3,
				MaxInterval:         time.Minute,
				MaxRetries:          5,
			},
		},
		{
			desc: "some fields configured",
			config: `
- namespace: .*
  type: docker
  backoff:
    initial_interval: 2s
    max_retries: 6
`,
			want: partial,
		},
		{
			desc: "enabled set to false",
			config: `
- namespace: .*
  type: docker
  backoff:
    enabled: false
`,
			want: defaults,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			require := require.New(t)

			var configs []Config
			require.NoError(yaml.Unmarshal([]byte(tt.config), &configs))

			m, err := NewMap(configs, nil)
			require.NoError(err)

			r, ok := m.subResolvers[0].resolver.(*dockerResolver)
			require.True(ok)
			require.Equal(tt.want, r.backoffConfig)
		})
	}
}
