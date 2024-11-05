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
package backend_test

import (
	"errors"
	"testing"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	. "github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
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

			require.NoError(m.Register("static", c1, false))
			require.NoError(m.Register("namespace-foo/.*", c2, false))

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
	require.NoError(m.Register("static", c, false))
	require.Error(m.Register("static", c, false))
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

	m, err := NewManager(configs, AuthConfig{}, tally.NoopScope)
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
	}}, AuthConfig{}, tally.NoopScope)
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

func TestManagerIsReady(t *testing.T) {
	const isReadyNamespace = "isReadyNamespace"
	const isReadyName = "38a03d499119bc417b8a6a016f2cb4540b9f9cc0c13e4da42a73867120d3e908"

	n1 := "foo/*"
	n2 := "bar/*"

	tests := []struct {
		name         string
		mustReady1   bool
		mustReady2   bool
		mockStat1Err error
		mockStat2Err error
		expectedRes  bool
		expectedErr  error
	}{
		{
			name:         "both required, both pass (one with nil, one with NotFound)",
			mustReady1:   true,
			mustReady2:   true,
			mockStat1Err: nil,
			mockStat2Err: backenderrors.ErrBlobNotFound,
			expectedRes:  true,
			expectedErr:  nil,
		},
		{
			name:         "both required, only second fails",
			mustReady1:   true,
			mustReady2:   true,
			mockStat1Err: nil,
			mockStat2Err: errors.New("network error"),
			expectedRes:  false,
			expectedErr:  errors.New("backend for namespace bar/* not ready: network error"),
		},
		{
			name:         "second required, only first fails",
			mustReady1:   false,
			mustReady2:   true,
			mockStat1Err: errors.New("network error"),
			mockStat2Err: backenderrors.ErrBlobNotFound,
			expectedRes:  true,
			expectedErr:  nil,
		},
	}

	for _, tc := range tests {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		c1 := mockbackend.NewMockClient(ctrl)
		c2 := mockbackend.NewMockClient(ctrl)

		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			m := ManagerFixture()

			mockStat1 := &core.BlobInfo{}
			mockStat2 := &core.BlobInfo{}
			if tc.mockStat1Err != nil {
				mockStat1 = nil
			}
			if tc.mockStat2Err != nil {
				mockStat2 = nil
			}

			c1.EXPECT().Stat(isReadyNamespace, isReadyName).Return(mockStat1, tc.mockStat1Err).AnyTimes()
			c2.EXPECT().Stat(isReadyNamespace, isReadyName).Return(mockStat2, tc.mockStat2Err).AnyTimes()

			require.NoError(m.Register(n1, c1, tc.mustReady1))
			require.NoError(m.Register(n2, c2, tc.mustReady2))

			res, err := m.IsReady()
			require.Equal(tc.expectedRes, res)
			if tc.expectedErr == nil {
				require.NoError(err)
			} else {
				require.Equal(tc.expectedErr, err)
			}
		})
	}
}
