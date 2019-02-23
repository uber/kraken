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
package heap

import (
	"container/heap"
	"errors"
)

// An Item is something we manage in a priority queue.
type Item struct {
	Value    interface{} // The value of the item; arbitrary.
	Priority int         // The priority of the item in the queue (low value == high priority).
}

// PriorityQueue implements a heap returns Items with lowest priority first.
type PriorityQueue struct {
	q internalQueue
}

// NewPriorityQueue initializes a PriorityQueue with passed items.
func NewPriorityQueue(items ...*Item) *PriorityQueue {
	q := internalQueue(items)
	heap.Init(&q)
	return &PriorityQueue{q}
}

// Len returns the number of Items in the PriorityQueue.
func (pq *PriorityQueue) Len() int { return len(pq.q) }

// Push adds the Item to the PriorityQueue.
func (pq *PriorityQueue) Push(item *Item) {
	heap.Push(&pq.q, item)
}

// Pop removes and returns the lowest priority Item from the PriorityQueue.
func (pq *PriorityQueue) Pop() (*Item, error) {
	if len(pq.q) == 0 {
		return nil, errors.New("queue empty")
	}

	return heap.Pop(&pq.q).(*Item), nil
}

// An internalQueue implements heap.Interface and holds Items.
type internalQueue []*Item

func (q internalQueue) Len() int { return len(q) }

func (q internalQueue) Less(i, j int) bool {
	return q[i].Priority < q[j].Priority
}

func (q internalQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *internalQueue) Push(x interface{}) {
	item := x.(*Item)
	*q = append(*q, item)
}

func (q *internalQueue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[0 : n-1]
	return item
}
