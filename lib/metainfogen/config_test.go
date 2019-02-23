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
package metainfogen

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
)

func TestPieceLengthConfig(t *testing.T) {
	require := require.New(t)

	plConfig, err := newPieceLengthConfig(map[datasize.ByteSize]datasize.ByteSize{
		0:               datasize.MB,
		2 * datasize.GB: 4 * datasize.MB,
		4 * datasize.GB: 8 * datasize.MB,
	})
	require.NoError(err)

	require.Equal(int64(datasize.MB), plConfig.get(int64(datasize.GB)))
	require.Equal(int64(4*datasize.MB), plConfig.get(int64(2*datasize.GB)))
	require.Equal(int64(4*datasize.MB), plConfig.get(int64(3*datasize.GB)))
	require.Equal(int64(8*datasize.MB), plConfig.get(int64(4*datasize.GB)))
	require.Equal(int64(8*datasize.MB), plConfig.get(int64(8*datasize.GB)))
}
