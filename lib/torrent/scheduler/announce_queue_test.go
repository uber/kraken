package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnnounceQueueReadyAfterNextMovesDispatcherIntoQueue(t *testing.T) {
	require := require.New(t)
	q := newAnnounceQueue()
	d := &dispatcher{}

	q.Add(d)

	n, ok := q.Next()
	require.True(ok)
	require.Equal(d, n)

	_, ok = q.Next()
	require.False(ok)

	q.Ready(d)

	// Ensure we remove d from pending map once ready.
	require.False(q.pending[d])

	n, ok = q.Next()
	require.True(ok)
	require.Equal(d, n)
}

func TestAnnounceQueueDoneOnReadyDispatcherDeletesAfterNext(t *testing.T) {
	require := require.New(t)
	q := newAnnounceQueue()
	d := &dispatcher{}

	q.Add(d)

	q.Done(d)

	n, ok := q.Next()
	require.True(ok)
	require.Equal(d, n)

	// Ensure we remove d from done map once popped via Next.
	require.False(q.done[d])

	// Even after calling Ready, d should not be moved back into the queue.
	q.Ready(d)
	_, ok = q.Next()
	require.False(ok)
}

func TestAnnounceQueueDoneOnPendingDispatcherDeletesAfterNext(t *testing.T) {
	require := require.New(t)
	q := newAnnounceQueue()
	d := &dispatcher{}

	q.Add(d)

	n, ok := q.Next()
	require.True(ok)
	require.Equal(d, n)

	// Call Done while d is pending.
	q.Done(d)

	q.Ready(d)

	n, ok = q.Next()
	require.True(ok)
	require.Equal(d, n)

	// Ensure we remove d from done map once popped via Next.
	require.False(q.done[d])

	// Even after calling Ready, d should not be moved back into the queue.
	q.Ready(d)
	_, ok = q.Next()
	require.False(ok)
}

func TestAnnounceQueueEjectDeletesDispatcherInAllStates(t *testing.T) {
	tests := []struct {
		description string
		setup       func(*announceQueue, *dispatcher)
	}{
		{"dispatcher in middle of queue", func(q *announceQueue, d *dispatcher) {
			q.Add(&dispatcher{})
			q.Add(d)
			q.Add(&dispatcher{})
		}},
		{"dispatcher ready", func(q *announceQueue, d *dispatcher) {
			q.Add(d)
			q.Next()
		}},
		{"dispatcher done", func(q *announceQueue, d *dispatcher) {
			q.Add(d)
			q.Done(d)
		}},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require := require.New(t)

			q := newAnnounceQueue()
			d := &dispatcher{}
			test.setup(q, d)

			q.Eject(d)

			require.False(q.done[d])
			require.False(q.pending[d])
			for {
				n, ok := q.Next()
				if !ok {
					break
				}
				require.False(d == n)
			}
		})
	}
}
