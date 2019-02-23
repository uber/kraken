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
package dockerregistry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStartedAtMetadataSerialization(t *testing.T) {
	require := require.New(t)

	s := newStartedAtMetadata(time.Now())
	b, err := s.Serialize()
	require.NoError(err)

	var result startedAtMetadata
	require.NoError(result.Deserialize(b))
	require.Equal(s.time.Unix(), result.time.Unix())
}

func TestHashState(t *testing.T) {
	require := require.New(t)

	h := newHashStateMetadata("sha256", "500")
	require.Equal(h.GetSuffix(), "_hashstates/sha256/500")
}
