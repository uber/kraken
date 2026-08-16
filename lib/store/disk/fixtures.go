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
package disk

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/uber/kraken/utils/memsize"
)

// Generously-sized capacity for Fixture, chosen so ordinary unit tests
// never trigger eviction.
const _fixtureCapacity = 100 * memsize.MB

// Fixture returns a Store backed by a temp directory, sized generously
// enough that unit tests won't trigger autoeviction.
func Fixture(t *testing.T) *Store {
	s, err := NewStore(&Config{
		Capacity:    _fixtureCapacity,
		RootDir:     t.TempDir(),
		ShardLength: 2,
	}, tally.NoopScope)
	require.NoError(t, err)
	return s
}
