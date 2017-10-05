package scheduler

import "container/list"

// announceQueue manages a queue of dispatchers waiting to announce.
// Not thread safe -- synchronization must be provided by clients.
type announceQueue struct {
	// Main queue of dispatchers ready to announce.
	readyQueue *list.List

	// Set of dispatchers with pending announce requests.
	pending map[*dispatcher]bool

	// Set of dispatchers to be deleted after their next announce.
	done map[*dispatcher]bool
}

func newAnnounceQueue() *announceQueue {
	return &announceQueue{
		readyQueue: list.New(),
		pending:    make(map[*dispatcher]bool),
		done:       make(map[*dispatcher]bool),
	}
}

// Next returns the next dispatcher ready to announce. After Next is called,
// the returned dispatcher will be marked as pending and will not be appear
// again in Next until Ready is called with said dispatcher. Second return
// value is false if no dispatchers are ready.
func (q *announceQueue) Next() (*dispatcher, bool) {
	next := q.readyQueue.Front()
	if next == nil {
		return nil, false
	}
	q.readyQueue.Remove(next)
	d := next.Value.(*dispatcher)
	if q.done[d] {
		delete(q.done, d)
	} else {
		q.pending[d] = true
	}
	return d, true
}

// Add adds a dispatcher to the front of the announce queue, so they can send
// their first announce as soon as possible. Behavior is undefined if called
// twice on the same dispatcher.
func (q *announceQueue) Add(d *dispatcher) {
	q.readyQueue.PushFront(d)
}

// Ready places a pending dispatcher back in the announce queue. Should be called
// once an announce response is received.
func (q *announceQueue) Ready(d *dispatcher) {
	if !q.pending[d] {
		return
	}
	delete(q.pending, d)
	q.readyQueue.PushBack(d)
}

// Done marks a dispatcher for deletion after its next announce.
func (q *announceQueue) Done(d *dispatcher) {
	q.done[d] = true
}
