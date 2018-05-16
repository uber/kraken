package base

import (
	"container/list"
	"os"
	"sync"
	"time"

	"code.uber.internal/infra/kraken/utils/log"

	"github.com/andres-erbsen/clock"
)

var _ FileMap = (*lruFileMap)(nil)

type fileEntryWithAccessTime struct {
	sync.RWMutex

	fe FileEntry

	// The last time that LoadForWrite/LoadForRead is called on the entry.
	lastAccessTime time.Time
}

// lruFileMap implements FileMap interface, with an optional max capacity, and
// will evict least recently accessed entry when the capacity is reached, which
// will only be updated by LoadForRead and LoadForWrite.
type lruFileMap struct {
	sync.Mutex

	// Capacity limit of the LRU map. Set capacity to 0 to disable eviction.
	size int

	clk clock.Clock

	// Min timespan between two updates of LAT for the same file.
	timeResolution time.Duration
	queue          *list.List
	elements       map[string]*list.Element
}

// NewLRUFileMap creates a new LRU map given capacity.
func NewLRUFileMap(size int, clk clock.Clock) (FileMap, error) {
	m := &lruFileMap{
		size:           size,
		clk:            clk,
		timeResolution: time.Minute * 5,
		queue:          list.New(),
		elements:       make(map[string]*list.Element),
	}

	return m, nil
}

// NewLATFileMap creates a new file map that tracks last access time, but no
// auto-eviction.
func NewLATFileMap(clk clock.Clock) (FileMap, error) {
	m := &lruFileMap{
		size:           0, // Disable eviction.
		clk:            clk,
		timeResolution: time.Minute * 5,
		queue:          list.New(),
		elements:       make(map[string]*list.Element),
	}

	return m, nil
}

func (fm *lruFileMap) get(name string) (*fileEntryWithAccessTime, bool) {
	if element, ok := fm.elements[name]; ok {
		fm.queue.MoveToFront(element)
		return element.Value.(*fileEntryWithAccessTime), ok
	}
	return nil, false
}

func (fm *lruFileMap) syncGet(name string) (*fileEntryWithAccessTime, bool) {
	fm.Lock()
	defer fm.Unlock()

	return fm.get(name)
}

func (fm *lruFileMap) syncGetAndTouch(name string) (*fileEntryWithAccessTime, bool) {
	fm.Lock()
	defer fm.Unlock()

	e, ok := fm.get(name)
	if !ok {
		return nil, false
	}

	// Update last access time.
	t := fm.clk.Now()
	if t.Sub(e.lastAccessTime) >= fm.timeResolution {
		// Only update if new timestamp is <timeResolution> newer than previous
		// value.
		e.lastAccessTime = t
		e.fe.SetMetadata(NewLastAccessTime(), MarshalLastAccessTime(t))
	}

	return e, true
}

func (fm *lruFileMap) add(name string, e *fileEntryWithAccessTime) bool {
	if _, ok := fm.elements[name]; !ok {
		element := fm.queue.PushFront(e)
		fm.elements[name] = element
		return true
	}
	return false
}

func (fm *lruFileMap) getOldest() (*fileEntryWithAccessTime, bool) {
	if e := fm.queue.Back(); e != nil {
		return e.Value.(*fileEntryWithAccessTime), true
	}
	return nil, false
}

func (fm *lruFileMap) remove(name string) (*fileEntryWithAccessTime, bool) {
	if e, ok := fm.elements[name]; ok {
		delete(fm.elements, name)
		fm.queue.Remove(e)
		return e.Value.(*fileEntryWithAccessTime), ok
	}
	return nil, false
}

func (fm *lruFileMap) syncRemove(name string) (*fileEntryWithAccessTime, bool) {
	fm.Lock()
	defer fm.Unlock()

	return fm.remove(name)
}

func (fm *lruFileMap) syncRemoveOldestIfNeeded() (e *fileEntryWithAccessTime, ok bool) {
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
	name := e.fe.GetName()
	if ne, ok := fm.syncGet(name); !ok {
		return nil, false
	} else if ne != e {
		return nil, false
	}

	if err := e.fe.Delete(); err != nil {
		log.With("name", e.fe.GetName()).Errorf("Error deleting evicted entry: %s", err)
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
	name string, entry FileEntry, f func(string, FileEntry) error) (FileEntry, bool) {
	// Lock on entry first, in case the lock is taken by other goroutine before f().
	e := &fileEntryWithAccessTime{
		fe: entry,
	}

	// After store, make sure size limit wasn't exceeded.
	// Also make sure this happens after e.RUnlock(), in case the new entry is to be deleted.
	defer fm.syncRemoveOldestIfNeeded()

	e.Lock()
	defer e.Unlock()

	fm.Lock()
	// Verify if it's already in the map.
	if actual, ok := fm.get(name); ok {
		defer fm.Unlock()
		return actual.fe, true
	}

	// Add new entry to map.
	fm.add(name, e)
	fm.Unlock()

	t := fm.clk.Now()
	b := MarshalLastAccessTime(t)
	if pb, err := e.fe.GetMetadata(NewLastAccessTime()); err != nil {
		// Set LAT if it doesn't exist on disk or cannot be read.
		if !os.IsNotExist(err) {
			log.With("name", e.fe.GetName()).Errorf("Error reading LAT: %s", err)
		}
		if _, err := e.fe.SetMetadata(NewLastAccessTime(), b); err != nil {
			log.With("name", e.fe.GetName()).Errorf("Error setting LAT: %s", err)
		}
		e.lastAccessTime = t
	} else {
		// This file is reloaded from disk, don't touch LAT if possible.
		if prevT, err := UnmarshalLastAccessTime(pb); err != nil {
			log.With("name", e.fe.GetName()).Errorf("Error parsing LAT: %s", err)
			// LAT data cannot be parsed, use new timestamp.
			if _, err := e.fe.SetMetadata(NewLastAccessTime(), b); err != nil {
				log.With("name", e.fe.GetName()).Errorf("Error setting LAT: %s", err)
			}
			e.lastAccessTime = t
		} else {
			e.lastAccessTime = prevT
		}
	}

	if err := f(name, e.fe); err != nil {
		// Remove from map while the entry lock is still being held.
		fm.syncRemove(name)
		return nil, false
	}

	return e.fe, false
}

// LoadForWrite looks up the value of key k and executes f under the protection
// of RLock.
// While f executes, it is guaranteed that k will not be deleted from the map.
// Returns false if k was not found.
// It updates last access time and file size.
func (fm *lruFileMap) LoadForWrite(name string, f func(string, FileEntry)) bool {
	e, ok := fm.syncGet(name)
	if !ok {
		return false
	}

	e.Lock()
	defer e.Unlock()

	// Now that we have the entry lock, make sure k was not deleted or
	// overwritten.
	if ne, ok := fm.syncGetAndTouch(name); !ok {
		return false
	} else if ne != e {
		return false
	}

	f(name, e.fe)

	return true
}

// LoadForRead looks up the value of key k and executes f under the protection
// of RLock.
// While f executes, it is guaranteed that k will not be deleted from the map.
// Returns false if k was not found.
// It updates last access time.
func (fm *lruFileMap) LoadForRead(name string, f func(string, FileEntry)) bool {
	e, ok := fm.syncGet(name)
	if !ok {
		return false
	}

	e.RLock()
	defer e.RUnlock()

	// Now that we have the entry lock, make sure k was not deleted or
	// overwritten.
	if ne, ok := fm.syncGetAndTouch(name); !ok {
		return false
	} else if ne != e {
		return false
	}

	f(name, e.fe)

	return true
}

// LoadForPeek looks up the value of key k and executes f under the protection
// of RLock.
// While f executes, it is guaranteed that k will not be deleted from the map.
// Returns false if k was not found.
func (fm *lruFileMap) LoadForPeek(name string, f func(string, FileEntry)) bool {
	e, ok := fm.syncGet(name)
	if !ok {
		return false
	}

	e.RLock()
	defer e.RUnlock()

	// Now that we have the entry lock, make sure k was not deleted or
	// overwritten.
	if ne, ok := fm.syncGet(name); !ok {
		return false
	} else if ne != e {
		return false
	}

	f(name, e.fe)

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

	// Now that we have the entry lock, make sure k was not deleted or
	// overwritten.
	if ne, ok := fm.syncGet(name); !ok {
		return false
	} else if ne != e {
		return false
	}

	if err := f(name, e.fe); err != nil {
		return false
	}

	// Remove from map while the entry lock is still being held.
	fm.syncRemove(name)

	return true
}
