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
package tiered

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/lib/store/memory"
	"github.com/uber/kraken/utils/memsize"
)

// Generously-sized capacities for StoreFixture, chosen so ordinary unit
// tests never trigger eviction.
const (
	_fixtureDiskCapacity = 100 * memsize.MB
	_fixtureMemCapacity  = 50 * memsize.MB
)

// StoreFixture returns a Store (and its underlying disk.Store) backed by a
// temp directory, sized generously enough that unit tests won't trigger
// autoeviction.
func StoreFixture(t *testing.T) (*Store, *disk.Store) {
	s, diskStore, err := NewStore(&Config{
		DiskConfig: &disk.Config{
			Capacity:              _fixtureDiskCapacity,
			RootDir:               t.TempDir(),
			RebootIncompleteBlobs: false,
			ShardLength:           2,
		},
		MemConfig: &memory.Config{
			CapacityBytes:   _fixtureMemCapacity,
			GOMEMLIMITBytes: math.MaxInt64,
		},
		NumFlushWorkers: 4,
	}, tally.NoopScope)
	require.NoError(t, err)

	// Close blocks until any pending mem-to-disk flush finishes, so
	// t.TempDir()'s cleanup (registered before this one, so it runs after,
	// per t.Cleanup's LIFO order) never races a background write.
	t.Cleanup(s.Close)

	return s, diskStore
}
