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
package hostlist

import (
	"testing"

	"github.com/uber/kraken/utils/stringset"

	"github.com/stretchr/testify/require"
)

func TestListResolve(t *testing.T) {
	require := require.New(t)

	addrs := []string{"a:80", "b:80", "c:80"}

	l, err := New(Config{Static: addrs})
	require.NoError(err)

	require.ElementsMatch(addrs, l.Resolve().ToSlice())
}

func TestAttachPortIfMissing(t *testing.T) {
	addrs, err := attachPortIfMissing(stringset.New("x", "y:5", "z"), 7)
	require.NoError(t, err)
	require.Equal(t, stringset.New("x:7", "y:5", "z:7"), addrs)
}

func TestAttachPortIfMissingError(t *testing.T) {
	_, err := attachPortIfMissing(stringset.New("a:b:c"), 7)
	require.Error(t, err)
}

func TestInvalidConfig(t *testing.T) {
	tests := []struct {
		desc   string
		config Config
	}{
		{"dns missing port", Config{DNS: "some-dns"}},
		{"static missing port", Config{Static: []string{"a:80", "b"}}},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			_, err := New(test.config)
			require.Error(t, err)
		})
	}
}
