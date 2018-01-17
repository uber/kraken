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

func TestQueueDoneOnReadyTorrentDeletesAfterNext(t *testing.T) {
	require := require.New(t)
	q := New()
	h := torlib.InfoHashFixture()

	q.Add(h)

	q.Done(h)

	n, ok := q.Next()
	require.True(ok)
	require.Equal(h, n)

	// Ensure we remove h from done map once popped via Next.
	require.False(q.done[h])

	// Even after calling Ready, h should not be moved back into the queue.
	q.Ready(h)
	_, ok = q.Next()
	require.False(ok)
}

func TestQueueDoneOnPendingTorrentDeletesAfterNext(t *testing.T) {
	require := require.New(t)
	q := New()
	h := torlib.InfoHashFixture()

	q.Add(h)

	n, ok := q.Next()
	require.True(ok)
	require.Equal(h, n)

	// Call Done while h is pending.
	q.Done(h)

	q.Ready(h)

	n, ok = q.Next()
	require.True(ok)
	require.Equal(h, n)

	// Ensure we remove h from done map once popped via Next.
	require.False(q.done[h])

	// Even after calling Ready, h should not be moved back into the queue.
	q.Ready(h)
	_, ok = q.Next()
	require.False(ok)
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
		{"torrent done", func(q *QueueImpl, h torlib.InfoHash) {
			q.Add(h)
			q.Done(h)
		}},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require := require.New(t)

			q := New()
			h := torlib.InfoHashFixture()
			test.setup(q, h)

			q.Eject(h)

			require.False(q.done[h])
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
