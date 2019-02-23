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
	"container/list"

	"github.com/uber/kraken/core"
)

// Queue manages a queue of torrents waiting to announce.
type Queue interface {
	Next() (core.InfoHash, bool)
	Add(core.InfoHash)
	Ready(core.InfoHash)
	Eject(core.InfoHash)
}

// QueueImpl is the primary implementation of Queue. QueueImpl is not thread
// safe -- synchronization must be provided by clients.
type QueueImpl struct {
	// Main queue of torrents ready to announce.
	readyQueue *list.List

	// Set of torrents with pending announce requests.
	pending map[core.InfoHash]bool
}

// New returns a new QueueImpl.
func New() *QueueImpl {
	return &QueueImpl{
		readyQueue: list.New(),
		pending:    make(map[core.InfoHash]bool),
	}
}

// Next returns the next torrent ready to announce. After Next is called,
// the returned torrent will be marked as pending and will not be appear
// again in Next until Ready is called with said torrent. Second return
// value is false if no torrents are ready.
func (q *QueueImpl) Next() (core.InfoHash, bool) {
	next := q.readyQueue.Front()
	if next == nil {
		return core.InfoHash{}, false
	}
	q.readyQueue.Remove(next)
	h := next.Value.(core.InfoHash)
	q.pending[h] = true
	return h, true
}

// Add adds a torrent to the back of the queue. Behavior is undefined if called
// twice on the same torrent.
func (q *QueueImpl) Add(h core.InfoHash) {
	q.readyQueue.PushBack(h)
}

// Ready places a pending torrent back in the queue. Should be called once an
// announce response is received.
func (q *QueueImpl) Ready(h core.InfoHash) {
	if !q.pending[h] {
		return
	}
	delete(q.pending, h)
	q.readyQueue.PushBack(h)
}

// Eject immediately ejects h from the announce queue, preventing it from
// announcing further.
func (q *QueueImpl) Eject(h core.InfoHash) {
	delete(q.pending, h)
	for e := q.readyQueue.Front(); e != nil; e = e.Next() {
		if e.Value.(core.InfoHash) == h {
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
func (q DisabledQueue) Next() (core.InfoHash, bool) { return core.InfoHash{}, false }

// Add noops.
func (q DisabledQueue) Add(core.InfoHash) {}

// Ready noops.
func (q DisabledQueue) Ready(core.InfoHash) {}

// Eject noops.
func (q DisabledQueue) Eject(core.InfoHash) {}
