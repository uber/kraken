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
package rwutil

import (
	"testing"

	"bytes"

	"github.com/stretchr/testify/require"
)

func TestCappedBuffer_write_drain_success(t *testing.T) {
	require := require.New(t)

	content := []byte("hello this is a stream of bytes")
	buffer := NewCappedBuffer(len(content))
	buffer.WriteAt(content[7:], 7)
	buffer.WriteAt(content[:7], 0)

	var dst bytes.Buffer
	buffer.DrainInto(&dst)
	require.Equal(content, dst.Bytes())
}

func TestCappedBuffer_write_drain_error(t *testing.T) {
	require := require.New(t)

	content := []byte("hello this is a stream of bytes")
	buffer := NewCappedBuffer(len(content) - 10)
	_, err := buffer.WriteAt(content[7:], 7)
	require.Error(err)

	_, err = buffer.WriteAt(content[:7], 0)
	require.NoError(err)
}
