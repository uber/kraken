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
package store

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/uber/kraken/core"

	"github.com/stretchr/testify/require"
)

func TestSimpleStoreCreateCacheFile(t *testing.T) {
	require := require.New(t)

	s, cleanup := SimpleStoreFixture()
	defer cleanup()

	tag := core.TagFixture()
	d := core.DigestFixture().String()

	require.NoError(s.CreateCacheFile(tag, bytes.NewBufferString(d)))

	f, err := s.GetCacheFileReader(tag)
	require.NoError(err)
	result, err := ioutil.ReadAll(f)
	require.NoError(err)
	require.Equal(d, string(result))
}
