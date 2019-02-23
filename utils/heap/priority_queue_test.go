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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPriorityQueue(t *testing.T) {
	require := require.New(t)
	items := []*Item{{"a", 3}, {"b", 2}, {"c", 4}}
	itemsCopy := []*Item{{"a", 3}, {"b", 2}, {"c", 4}}

	pq := NewPriorityQueue(items...)

	var item *Item
	var err error

	item, err = pq.Pop()
	require.NoError(err)
	require.Equal(itemsCopy[1], item)

	newItem := &Item{"d", 1}
	pq.Push(newItem)

	item, err = pq.Pop()
	require.NoError(err)
	require.Equal(newItem, item)

	item, err = pq.Pop()
	require.NoError(err)
	require.Equal(itemsCopy[0], item)

	item, err = pq.Pop()
	require.NoError(err)
	require.Equal(itemsCopy[2], item)

	_, err = pq.Pop()
	require.Error(err)
}
