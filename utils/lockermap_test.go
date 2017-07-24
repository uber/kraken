package utils

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockEntry struct {
	sync.Mutex

	intVal  int
	mapVal  map[string][]byte
	chanVal *chan uint8
}

func (e *mockEntry) equal(e1 *mockEntry) bool {
	if e.intVal != e1.intVal {
		return false
	}

	if len(e.mapVal) != len(e1.mapVal) {
		return false
	}

	for k, v := range e.mapVal {
		v1, ok := e1.mapVal[k]
		if !ok || string(v) != string(v1) {
			return false
		}
	}

	return true
}

func TestLockerMap(t *testing.T) {
	m := NewLockerMap()

	c := make(chan byte)

	entry1 := &mockEntry{
		intVal: 1,
		mapVal: map[string][]byte{
			"bytes": {
				'a',
				'b',
				'c',
			},
		},
		chanVal: &c,
	}

	entry2 := &mockEntry{
		intVal: 45,
		mapVal: map[string][]byte{
			"bytes": {
				'e',
				'f',
				'g',
			},
		},
		chanVal: &c,
	}

	go func() {
		c <- uint8(0)
		c <- uint8(1)
	}()

	// store
	assert.Nil(t, m.Store(1, entry1, func(e sync.Locker) {
		assert.True(t, entry1.equal(e.(*mockEntry)))
		val := <-*(e.(*mockEntry).chanVal)
		assert.Equal(t, uint8(0), val)
	}))

	// store duplicated
	assert.Equal(t, "LockerMap error: Unable to store. Duplicated key",
		m.Store(1, entry1, func(e sync.Locker) {}).Error())

	// load
	assert.Nil(t, m.Load(1, func(e sync.Locker) {
		assert.True(t, entry1.equal(e.(*mockEntry)))
		val := <-*(e.(*mockEntry).chanVal)
		assert.Equal(t, uint8(1), val)
	}))

	// load not found
	assert.Equal(t, "LockerMap error: Unable to load. Key does not exist",
		m.Load(2, func(e sync.Locker) {}).Error())

	// store another
	assert.Nil(t, m.Store(2, entry2, func(e sync.Locker) {
		assert.True(t, entry2.equal(e.(*mockEntry)))
	}))

	// load another
	assert.Nil(t, m.Load(2, func(e sync.Locker) {
		assert.True(t, entry2.equal(e.(*mockEntry)))
	}))

	// range
	var sum int
	m.Range(func(val sync.Locker) bool {
		sum = sum + val.(*mockEntry).intVal
		return true
	})
	assert.Equal(t, 46, sum)

	// delete
	m.Delete(2)
	assert.Equal(t, "LockerMap error: Unable to load. Key does not exist",
		m.Load(2, func(e sync.Locker) {}).Error())

	sum = 0
	m.Range(func(val sync.Locker) bool {
		sum = sum + val.(*mockEntry).intVal
		return true
	})
	assert.Equal(t, 1, sum)
}
