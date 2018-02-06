package announcequeue

import (
	"testing"

	"code.uber.internal/infra/kraken/torlib"

	"github.com/stretchr/testify/require"
)

func TestQueueReadyAfterNextMovesTorrentIntoQueue(t *testing.T) {
	require := require.New(t)
	q := New()
	h := torlib.InfoHashFixture()

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
		setup       func(*QueueImpl, torlib.InfoHash)
	}{
		{"torrent in middle of queue", func(q *QueueImpl, h torlib.InfoHash) {
			q.Add(torlib.InfoHashFixture())
			q.Add(h)
			q.Add(torlib.InfoHashFixture())
		}},
		{"torrent ready", func(q *QueueImpl, h torlib.InfoHash) {
			q.Add(h)
			q.Next()
		}},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require := require.New(t)

			q := New()
			h := torlib.InfoHashFixture()
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
