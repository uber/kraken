// Copyright (c) 2019 Uber Technologies, Inc.
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
	"bytes"
	"io"
	"testing"

	"github.com/uber/kraken/utils/randutil"
	"github.com/stretchr/testify/require"
)

func TestPlainReader(t *testing.T) {
	require := require.New(t)

	data := randutil.Text(32)

	var result bytes.Buffer
	_, err := io.Copy(&result, PlainReader(data))
	require.NoError(err)
	require.Equal(data, result.Bytes())
}

func TestPlainWriter(t *testing.T) {
	require := require.New(t)

	data := randutil.Text(32)

	w := make(PlainWriter, len(data))
	_, err := io.Copy(w, bytes.NewReader(data))
	require.NoError(err)
	require.Equal(data, []byte(w))
}
