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
