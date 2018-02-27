package base

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"code.uber.internal/infra/kraken/utils/log"

	"github.com/andres-erbsen/clock"
)

var _ FileMap = (*lruFileMap)(nil)

// lruFileMap implements FileMap interface
type lruFileMap struct {
	sync.Mutex

	size           int // Size limit of the LRU map. It's not a strict limit, could be exceeded briefly.
	clk            clock.Clock
	timeResolution time.Duration // Min timespan between two updates of last access time for the same file.
	queue          *list.List
	elements       map[string]*list.Element
}

// NewLRUFileMap creates a new LRU map given size.
func NewLRUFileMap(size int, clk clock.Clock) (FileMap, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid lru map size: %d", size)
	}

	m := &lruFileMap{
		size:           size,
		clk:            clk,
		timeResolution: time.Minute * 5,
		queue:          list.New(),
		elements:       make(map[string]*list.Element),
	}

	return m, nil
}

func (fm *lruFileMap) get(name string) (FileEntry, bool) {
	if element, ok := fm.elements[name]; ok {
		fm.queue.MoveToFront(element)
		e := element.Value.(FileEntry)
		return e, true
	}
	return nil, false
}

func (fm *lruFileMap) syncGet(name string) (FileEntry, bool) {
	fm.Lock()
	defer fm.Unlock()

	return fm.get(name)
}

func (fm *lruFileMap) add(name string, e FileEntry) bool {
	if _, ok := fm.elements[name]; !ok {
		element := fm.queue.PushFront(e)
		fm.elements[name] = element
		return true
	}
	return false
}

func (fm *lruFileMap) getOldest() (FileEntry, bool) {
	if e := fm.queue.Back(); e != nil {
		return e.Value.(FileEntry), true
	}
	return nil, false
}

func (fm *lruFileMap) remove(name string) (FileEntry, bool) {
	if e, ok := fm.elements[name]; ok {
		delete(fm.elements, name)
		fm.queue.Remove(e)
		return e.Value.(FileEntry), ok
	}
	return nil, false
}

func (fm *lruFileMap) syncRemove(name string) (FileEntry, bool) {
	fm.Lock()
	defer fm.Unlock()

	return fm.remove(name)
}

func (fm *lruFileMap) syncRemoveOldestIfNeeded() (e FileEntry, ok bool) {
	// Verify if size limit was defined and exceeded.
	fm.Lock()
	if fm.size <= 0 || fm.queue.Len() <= fm.size {
		defer fm.Unlock()
		return nil, false
	}
	e, ok = fm.getOldest()
	if !ok {
		defer fm.Unlock()
		return nil, false
	}
	fm.Unlock()

	e.Lock()
	defer e.Unlock()

	// Now that we have the entry lock, make sure k was not deleted or overwritten.
	name := e.GetName()
	if ne, ok := fm.syncGet(name); !ok {
		return nil, false
	} else if ne != e {
		return nil, false
	}

	if err := e.Delete(); err != nil {
		log.With("name", e.GetName()).Errorf("Error deleting evicted entry: %s", err)
	}

	// Remove from map while the entry lock is still being held
	fm.syncRemove(name)

	return e, true
}

// Contains returns true if the given key is stored in the map.
func (fm *lruFileMap) Contains(name string) bool {
	fm.Lock()
	defer fm.Unlock()

	_, ok := fm.elements[name]
	return ok
}

// LoadOrStore tries to stores the given key / value pair into the map.
// If object is successfully stored, execute f under the protection of RLock.
// Returns existing oject and true if the name is already present.
func (fm *lruFileMap) LoadOrStore(
	name string, e FileEntry, f func(string, FileEntry) error) (FileEntry, bool) {
	// Lock on entry first, in case the lock is taken by other goroutine before f().

	// After store, make sure size limit wasn't exceeded.
	// Also make sure this happens after e.RUnlock(), in case the new entry is the one to be deleted.
	defer fm.syncRemoveOldestIfNeeded()

	e.RLock()
	defer e.RUnlock()

	fm.Lock()
	// Verify if it's already in the map.
	if actual, ok := fm.get(name); ok {
		defer fm.Unlock()
		return actual, true
	}
	// Add new entry to map.
	fm.add(name, e)
	fm.Unlock()

	if err := f(name, e); err != nil {
		// Remove from map while the entry lock is still being held
		fm.syncRemove(name)
		return nil, false
	}

	return e, false
}

// LoadReadOnly looks up the value of key k and executes f under the protection of RLock.
// While f executes, it is guaranteed that k will not be deleted from the map.
// Returns false if k was not found.
func (fm *lruFileMap) LoadReadOnly(name string, f func(string, FileEntry)) bool {
	e, ok := fm.syncGet(name)
	if !ok {
		return false
	}

	e.RLock()
	defer e.RUnlock()

	// Now that we have the entry lock, make sure k was not deleted or overwritten.
	if ne, ok := fm.syncGet(name); !ok {
		return false
	} else if ne != e {
		return false
	}

	f(name, e)

	return true
}

// Load looks up the value of key k and executes f under the protection of RLock.
// While f executes, it is guaranteed that k will not be deleted from the map.
// Returns false if k was not found.
func (fm *lruFileMap) Load(name string, f func(string, FileEntry)) bool {
	e, ok := fm.syncGet(name)
	if !ok {
		return false
	}

	e.Lock()
	defer e.Unlock()

	// Now that we have the entry lock, make sure k was not deleted or overwritten.
	if ne, ok := fm.syncGet(name); !ok {
		return false
	} else if ne != e {
		return false
	}

	f(name, e)

	return true
}

// Delete deletes the given key from the Map.
// It also executes f under the protection of Lock.
// If f returns false, abort before key deletion.
func (fm *lruFileMap) Delete(name string, f func(string, FileEntry) error) bool {
	e, ok := fm.syncGet(name)
	if !ok {
		return false
	}

	e.Lock()
	defer e.Unlock()

	// Now that we have the entry lock, make sure k was not deleted or overwritten.
	if ne, ok := fm.syncGet(name); !ok {
		return false
	} else if ne != e {
		return false
	}

	if err := f(name, e); err != nil {
		return false
	}

	// Remove from map while the entry lock is still being held
	fm.syncRemove(name)

	return true
}
