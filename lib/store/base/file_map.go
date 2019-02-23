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
package base

import (
	"container/list"
	"os"
	"sync"
	"time"

	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/log"

	"github.com/andres-erbsen/clock"
)

// FileMap is a thread-safe name -> FileEntry map.
type FileMap interface {
	Contains(name string) bool
	TryStore(name string, entry FileEntry, f func(string, FileEntry) bool) bool
	LoadForWrite(name string, f func(string, FileEntry)) bool
	LoadForRead(name string, f func(string, FileEntry)) bool
	LoadForPeek(name string, f func(string, FileEntry)) bool
	Delete(name string, f func(string, FileEntry) bool) bool
}

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
func NewLRUFileMap(size int, clk clock.Clock) FileMap {
	m := &lruFileMap{
		size:           size,
		clk:            clk,
		timeResolution: time.Minute * 5,
		queue:          list.New(),
		elements:       make(map[string]*list.Element),
	}

	return m
}

// NewLATFileMap creates a new file map that tracks last access time, but no
// auto-eviction.
func NewLATFileMap(clk clock.Clock) FileMap {
	m := &lruFileMap{
		size:           0, // Disable eviction.
		clk:            clk,
		timeResolution: time.Minute * 5,
		queue:          list.New(),
		elements:       make(map[string]*list.Element),
	}

	return m
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
		e.fe.SetMetadata(metadata.NewLastAccessTime(t))
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

	// Now that we have the entry lock, make sure k was not deleted or
	// overwritten.
	name := e.fe.GetName()
	if ne, ok := fm.syncGet(name); !ok {
		return nil, false
	} else if ne != e {
		return nil, false
	}

	if err := e.fe.Delete(); err != nil {
		log.With("name", e.fe.GetName()).Errorf("Error deleting evicted entry: %s", err)
	}

	// Remove from map while the entry lock is still being held.
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

// TryStore tries to stores the given key / value pair into the map.
// If object is successfully stored, execute f under the protection of Lock.
// Returns false if the name is already present.
func (fm *lruFileMap) TryStore(name string, entry FileEntry, f func(string, FileEntry) bool) bool {
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
	if _, ok := fm.get(name); ok {
		defer fm.Unlock()

		// Update last access time.
		t := fm.clk.Now()
		if t.Sub(e.lastAccessTime) >= fm.timeResolution {
			// Only update if new timestamp is <timeResolution> newer than
			// previous value.
			e.lastAccessTime = t
			e.fe.SetMetadata(metadata.NewLastAccessTime(t))
		}

		return false
	}

	// Add new entry to map.
	fm.add(name, e)

	lat := metadata.NewLastAccessTime(fm.clk.Now())
	if err := e.fe.GetMetadata(lat); err != nil {
		// Set LAT if it doesn't exist on disk or cannot be read.
		if !os.IsNotExist(err) {
			log.With("name", e.fe.GetName()).Errorf("Error reading LAT: %s", err)
		}
		if _, err := e.fe.SetMetadata(lat); err != nil {
			log.With("name", e.fe.GetName()).Errorf("Error setting LAT: %s", err)
		}
	}
	e.lastAccessTime = lat.Time

	fm.Unlock()

	if !f(name, e.fe) {
		// Remove from map while the entry lock is still being held.
		fm.syncRemove(name)
		return false
	}

	return true
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
func (fm *lruFileMap) Delete(name string, f func(string, FileEntry) bool) bool {
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

	if !f(name, e.fe) {
		return false
	}

	// Remove from map while the entry lock is still being held.
	fm.syncRemove(name)

	return true
}
