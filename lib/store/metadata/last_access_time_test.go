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
package metadata

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLastAccessTimeSerialization(t *testing.T) {
	require := require.New(t)

	lat := NewLastAccessTime(time.Now().Add(-time.Hour))
	b, err := lat.Serialize()
	require.NoError(err)

	var newLat LastAccessTime
	require.NoError(newLat.Deserialize(b))
	require.Equal(lat.Time.Unix(), newLat.Time.Unix())
}
