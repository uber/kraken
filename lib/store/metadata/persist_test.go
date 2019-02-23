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
package metadata

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPersistMetadataSerialization(t *testing.T) {
	for _, v := range []bool{true, false} {
		t.Run(strconv.FormatBool(v), func(t *testing.T) {
			require := require.New(t)

			p := NewPersist(v)
			b, err := p.Serialize()
			require.NoError(err)

			var result Persist
			require.NoError(result.Deserialize(b))
			require.Equal(p.Value, result.Value)
		})
	}
}
