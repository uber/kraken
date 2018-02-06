package announcequeue

import (
	"container/list"

	"code.uber.internal/infra/kraken/torlib"
)

// Queue manages a queue of torrents waiting to announce.
type Queue interface {
	Next() (torlib.InfoHash, bool)
	Add(torlib.InfoHash)
	Ready(torlib.InfoHash)
	Eject(torlib.InfoHash)
}

// QueueImpl is the primary implementation of Queue. QueueImpl is not thread
// safe -- synchronization must be provided by clients.
type QueueImpl struct {
	// Main queue of torrents ready to announce.
	readyQueue *list.List

	// Set of torrents with pending announce requests.
	pending map[torlib.InfoHash]bool
}

// New returns a new QueueImpl.
func New() *QueueImpl {
	return &QueueImpl{
		readyQueue: list.New(),
		pending:    make(map[torlib.InfoHash]bool),
	}
}

// Next returns the next torrent ready to announce. After Next is called,
// the returned torrent will be marked as pending and will not be appear
// again in Next until Ready is called with said torrent. Second return
// value is false if no torrents are ready.
func (q *QueueImpl) Next() (torlib.InfoHash, bool) {
	next := q.readyQueue.Front()
	if next == nil {
		return torlib.InfoHash{}, false
	}
	q.readyQueue.Remove(next)
	h := next.Value.(torlib.InfoHash)
	q.pending[h] = true
	return h, true
}

// Add adds a torrent to the back of the queue. Behavior is undefined if called
// twice on the same torrent.
func (q *QueueImpl) Add(h torlib.InfoHash) {
	q.readyQueue.PushBack(h)
}

// Ready places a pending torrent back in the queue. Should be called once an
// announce response is received.
func (q *QueueImpl) Ready(h torlib.InfoHash) {
	if !q.pending[h] {
		return
	}
	delete(q.pending, h)
	q.readyQueue.PushBack(h)
}

// Eject immediately ejects h from the announce queue, preventing it from
// announcing further.
func (q *QueueImpl) Eject(h torlib.InfoHash) {
	delete(q.pending, h)
	for e := q.readyQueue.Front(); e != nil; e = e.Next() {
		if e.Value.(torlib.InfoHash) == h {
			q.readyQueue.Remove(e)
		}
	}
}

// DisabledQueue is a Queue which ignores all input and constantly returns that
// there are no torrents in the queue. Suitable for origin peers which want to
// disable announcing.
type DisabledQueue struct{}

// Disabled returns a new DisabledQueue.
func Disabled() DisabledQueue {
	return DisabledQueue{}
}

// Next never returns a torrent.
func (q DisabledQueue) Next() (torlib.InfoHash, bool) { return torlib.InfoHash{}, false }

// Add noops.
func (q DisabledQueue) Add(torlib.InfoHash) {}

// Ready noops.
func (q DisabledQueue) Ready(torlib.InfoHash) {}

// Eject noops.
func (q DisabledQueue) Eject(torlib.InfoHash) {}
