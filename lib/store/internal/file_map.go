package internal

import (
	"sync"

	"golang.org/x/sync/syncmap"
)

// FileMap is a thread-safe name -> FileEntry map.
type FileMap interface {
	Contains(name string) bool
	LoadOrStore(name string, entry FileEntry, f func(string, FileEntry) error) (FileEntry, bool)
	LoadReadOnly(name string, f func(string, FileEntry)) bool
	Load(name string, f func(string, FileEntry)) bool
	Delete(name string, f func(string, FileEntry) error) bool
}

var _ FileMap = (*simpleFileMap)(nil)

type fileEntryWithRWLock struct {
	sync.RWMutex

	fe FileEntry
}

// simpleFileMap is a two-level locking map which synchronizes access to the
// map in addition to synchronizing access to the values within the map. Useful
// for mutating values in-place.
//
// The zero Map is valid and empty.
type simpleFileMap struct {
	m syncmap.Map
}

// NewSimpleFileMap inits a new simpleFileMap object.
func NewSimpleFileMap() FileMap {
	return &simpleFileMap{}
}

// Contains returns true if the given key is stored in the map.
func (fm *simpleFileMap) Contains(name string) bool {
	_, loaded := fm.m.Load(name)

	return loaded
}

// LoadOrStore tries to stores the given key / value pair into the map.
// If entry was successfully put into the map, execute f under the protection of Lock.
// Returns existing oject and true if the name is already present.
func (fm *simpleFileMap) LoadOrStore(
	name string, entry FileEntry, f func(string, FileEntry) error) (FileEntry, bool) {
	// Grab entry lock first, in case other goroutines get the lock between LoadOrStore() and f().
	e := &fileEntryWithRWLock{
		fe: entry,
	}
	e.Lock()
	defer e.Unlock()

	if actual, loaded := fm.m.LoadOrStore(name, e); loaded {
		return actual.(*fileEntryWithRWLock).fe, true
	}

	if err := f(name, e.fe); err != nil {
		// Remove from map while the entry lock is still being held
		fm.m.Delete(name)
		return nil, false
	}
	return entry, false
}

// LoadReadOnly looks up the value of key k and executes f under the protection of RLock.
// While f executes, it is guaranteed that k will not be deleted from the map.
// Returns false if k was not found.
func (fm *simpleFileMap) LoadReadOnly(name string, f func(string, FileEntry)) bool {
	v, ok := fm.m.Load(name)
	if !ok {
		return false
	}

	e := v.(*fileEntryWithRWLock)
	e.RLock()
	defer e.RUnlock()

	// Now that we have the entry lock, make sure k was not deleted or overwritten.
	if nv, ok := fm.m.Load(name); !ok {
		return false
	} else if nv != v {
		return false
	}

	f(name, e.fe)

	return true
}

// Load looks up the value of key k and executes f under the protection of Lock.
// While f executes, it is guaranteed that k will not be deleted from the map.
// Returns false if k was not found.
func (fm *simpleFileMap) Load(name string, f func(string, FileEntry)) bool {
	v, ok := fm.m.Load(name)
	if !ok {
		return false
	}

	e := v.(*fileEntryWithRWLock)
	e.Lock()
	defer e.Unlock()

	// Now that we have the entry lock, make sure k was not deleted or overwritten.
	if nv, ok := fm.m.Load(name); !ok {
		return false
	} else if nv != v {
		return false
	}

	f(name, e.fe)

	return true
}

// Delete deletes the given key from the Map.
// It also executes f under the protection of Lock.
// If f returns false, abort before key deletion.
func (fm *simpleFileMap) Delete(name string, f func(string, FileEntry) error) bool {
	v, ok := fm.m.Load(name)
	if !ok {
		return false
	}

	e := v.(*fileEntryWithRWLock)
	e.Lock()
	defer e.Unlock()

	// Now that we have the entry lock, make sure k was not deleted or overwritten.
	if nv, ok := fm.m.Load(name); !ok {
		return false
	} else if nv != v {
		return false
	}

	if err := f(name, e.fe); err != nil {
		return false
	}

	fm.m.Delete(name)
	return true
}

// Range interates over the Map and execs f until f returns false.
func (fm *simpleFileMap) Range(f func(name string, fe FileEntry) bool) {
	fm.m.Range(func(k, v interface{}) bool {
		e := v.(*fileEntryWithRWLock)
		e.Lock()
		defer e.Unlock()

		// Verify and skip entry that has been deleted or overwritten.
		if nv, ok := fm.m.Load(k); !ok {
			return true
		} else if nv != v {
			return true
		}

		return f(k.(string), e.fe)
	})
}
