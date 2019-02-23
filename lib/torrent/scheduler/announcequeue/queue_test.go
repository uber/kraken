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
package announcequeue

import (
	"testing"

	"github.com/uber/kraken/core"

	"github.com/stretchr/testify/require"
)

func TestQueueReadyAfterNextMovesTorrentIntoQueue(t *testing.T) {
	require := require.New(t)
	q := New()
	h := core.InfoHashFixture()

	q.Add(h)

	n, ok := q.Next()
	require.True(ok)
	require.Equal(h, n)

	_, ok = q.Next()
	require.False(ok)

	q.Ready(h)

	// Ensure we remove h from pending map once ready.
	require.False(q.pending[h])

	n, ok = q.Next()
	require.True(ok)
	require.Equal(h, n)
}

func TestQueueEjectDeletesTorrentInAllStates(t *testing.T) {
	tests := []struct {
		description string
		setup       func(*QueueImpl, core.InfoHash)
	}{
		{"torrent in middle of queue", func(q *QueueImpl, h core.InfoHash) {
			q.Add(core.InfoHashFixture())
			q.Add(h)
			q.Add(core.InfoHashFixture())
		}},
		{"torrent ready", func(q *QueueImpl, h core.InfoHash) {
			q.Add(h)
			q.Next()
		}},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require := require.New(t)

			q := New()
			h := core.InfoHashFixture()
			test.setup(q, h)

			q.Eject(h)

			require.False(q.pending[h])
			for {
				n, ok := q.Next()
				if !ok {
					break
				}
				require.False(h == n)
			}
		})
	}
}
