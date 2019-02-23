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
package dispatch

import (
	"testing"

	"github.com/uber/kraken/utils/bitsetutil"

	"github.com/stretchr/testify/require"
)

func TestSyncBitfieldDuplicateSetDoesNotDoubleCount(t *testing.T) {
	require := require.New(t)

	b := newSyncBitfield(bitsetutil.FromBools(false, false))
	require.False(b.Complete())

	b.Set(0, true)
	require.False(b.Complete())
	b.Set(0, true)
	require.False(b.Complete())

	b.Set(1, true)
	require.True(b.Complete())

	b.Set(1, false)
	require.False(b.Complete())
	b.Set(1, false)
	require.False(b.Complete())

	b.Set(1, true)
	require.True(b.Complete())
}

func TestSyncBitfieldNewCountsNumComplete(t *testing.T) {
	require := require.New(t)

	b := newSyncBitfield(bitsetutil.FromBools(true, true, true))
	require.True(b.Complete())
}

func TestSyncBitfieldString(t *testing.T) {
	require := require.New(t)

	b := newSyncBitfield(bitsetutil.FromBools(true, false, true, false))
	require.Equal("1010", b.String())
}
