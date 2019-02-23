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
package backend_test

import (
	"testing"

	. "github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/namepath"
	"github.com/uber/kraken/lib/backend/testfs"
	"github.com/uber/kraken/mocks/lib/backend"
	"github.com/uber/kraken/utils/bandwidth"
	"github.com/uber/kraken/utils/stringset"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestManagerNamespaceMatching(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c1 := mockbackend.NewMockClient(ctrl)
	c2 := mockbackend.NewMockClient(ctrl)

	tests := []struct {
		namespace string
		expected  Client
	}{
		{"static", c1},
		{"namespace-foo/repo-bar", c2},
	}
	for _, test := range tests {
		t.Run(test.namespace, func(t *testing.T) {
			require := require.New(t)

			m := ManagerFixture()

			require.NoError(m.Register("static", c1))
			require.NoError(m.Register("namespace-foo/.*", c2))

			result, err := m.GetClient(test.namespace)
			require.NoError(err)
			require.True(
				test.expected.(*mockbackend.MockClient) == result.(*mockbackend.MockClient))
		})
	}
}

func TestManagerErrDuplicateNamespace(t *testing.T) {
	require := require.New(t)

	c := &NoopClient{}
	m := ManagerFixture()
	require.NoError(m.Register("static", c))
	require.Error(m.Register("static", c))
}

func TestManagerErrNamespaceNotFound(t *testing.T) {
	require := require.New(t)

	m := ManagerFixture()
	_, err := m.GetClient("no-match")
	require.Equal(ErrNamespaceNotFound, err)
}

func TestManagerNamespaceOrdering(t *testing.T) {
	require := require.New(t)

	fooBarAddr := "testfs-foo-bar"
	fooAddr := "testfs-foo"
	defaultAddr := "testfs-default"

	configStr := `
- namespace: foo/bar/.*
  backend:
      testfs:
          addr: testfs-foo-bar
          name_path: identity
- namespace: foo/.*
  backend:
      testfs:
          addr: testfs-foo
          name_path: identity
- namespace: .*
  backend:
      testfs:
          addr: testfs-default
          name_path: identity
`
	var configs []Config
	require.NoError(yaml.Unmarshal([]byte(configStr), &configs))

	m, err := NewManager(configs, AuthConfig{})
	require.NoError(err)

	for ns, expected := range map[string]string{
		"foo/bar/baz": fooBarAddr,
		"foo/bar/123": fooBarAddr,
		"foo/123":     fooAddr,
		"abc":         defaultAddr,
		"xyz":         defaultAddr,
		"bar/baz":     defaultAddr,
		"":            defaultAddr,
	} {
		c, err := m.GetClient(ns)
		require.NoError(err)
		require.Equal(expected, c.(*testfs.Client).Addr(), "Namespace: %s", ns)
	}
}

func TestManagerBandwidth(t *testing.T) {
	require := require.New(t)

	m, err := NewManager([]Config{{
		Namespace: ".*",
		Bandwidth: bandwidth.Config{
			EgressBitsPerSec:  10,
			IngressBitsPerSec: 50,
			TokenSize:         1,
			Enable:            true,
		},
		Backend: map[string]interface{}{
			"testfs": testfs.Config{Addr: "test-addr", NamePath: namepath.Identity},
		},
	}}, AuthConfig{})
	require.NoError(err)

	checkBandwidth := func(egress, ingress int64) {
		c, err := m.GetClient("foo")
		require.NoError(err)
		tc, ok := c.(*ThrottledClient)
		require.True(ok)
		require.Equal(egress, tc.EgressLimit())
		require.Equal(ingress, tc.IngressLimit())
	}

	checkBandwidth(10, 50)

	watcher := NewBandwidthWatcher(m)
	watcher.Notify(stringset.New("a", "b"))

	checkBandwidth(5, 25)
}
