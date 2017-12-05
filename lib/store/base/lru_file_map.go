package base

import (
	"fmt"
	"sync"

	"github.com/hashicorp/golang-lru/simplelru"

	"code.uber.internal/infra/kraken/utils/log"
)

var _ FileMap = (*lruFileMap)(nil)

// lruFileMap implements FileMap interface
type lruFileMap struct {
	sync.Mutex

	cache *simplelru.LRU
}

// NewLRUFileMap creates a new lru map given size
func NewLRUFileMap(size int) (FileMap, error) {
	if size <= 0 {
		return nil, fmt.Errorf("Invalid map size: %d", size)
	}

	c, err := simplelru.NewLRU(size, onEvictCallBack)
	if err != nil {
		return nil, err
	}

	m := &lruFileMap{
		cache: c,
	}

	return m, nil
}

func onEvictCallBack(key interface{}, value interface{}) {
	e := value.(*fileEntryWithRWLock)

	// Entry will be removed from the map, so we do not need to verify if it's still in map.
	// TODO: this happens without acquiring entry lock, so caller need to be able to handle unexpected deletion.
	if err := e.fe.Delete(); err != nil {
		log.Errorf("unable to delete file entry on cache eviction %s", err)
	}
}

func (fm *lruFileMap) load(key interface{}) (value interface{}, ok bool) {
	fm.Lock()
	defer fm.Unlock()

	return fm.cache.Get(key)
}

// Contains returns true if the given key is stored in the map.
func (fm *lruFileMap) Contains(name string) bool {
	fm.Lock()
	defer fm.Unlock()

	return fm.cache.Contains(name)
}

// LoadOrStore tries to stores the given key / value pair into the map.
// If object is successfully stored, execute f under the protection of RLock.
// Returns existing oject and true if the name is already present.
func (fm *lruFileMap) LoadOrStore(
	name string, entry FileEntry, f func(string, FileEntry) error) (FileEntry, bool) {
	// Lock on entry first, in case the lock is taken by other goroutine before f().
	e := &fileEntryWithRWLock{
		fe: entry,
	}
	e.RLock()
	defer e.RUnlock()

	fm.Lock()
	if actual, ok := fm.cache.Get(name); ok {
		defer fm.Unlock()
		return actual.(*fileEntryWithRWLock).fe, true
	}
	fm.cache.Add(name, e)
	fm.Unlock()

	if err := f(name, e.fe); err != nil {
		// Remove from map while the entry lock is still being held
		fm.Lock()
		fm.cache.Remove(name)
		fm.Unlock()
		return nil, false
	}

	return e.fe, false
}

// LoadReadOnly looks up the value of key k and executes f under the protection of RLock.
// While f executes, it is guaranteed that k will not be deleted from the map.
// Returns false if k was not found.
func (fm *lruFileMap) LoadReadOnly(name string, f func(string, FileEntry)) bool {
	v, ok := fm.load(name)
	if !ok {
		return false
	}

	e := v.(*fileEntryWithRWLock)
	e.RLock()
	defer e.RUnlock()

	// Now that we have the entry lock, make sure k was not deleted or overwritten.
	if nv, ok := fm.load(name); !ok {
		return false
	} else if nv != v {
		return false
	}

	f(name, e.fe)

	return true
}

// Load looks up the value of key k and executes f under the protection of RLock.
// While f executes, it is guaranteed that k will not be deleted from the map.
// Returns false if k was not found.
func (fm *lruFileMap) Load(name string, f func(string, FileEntry)) bool {
	v, ok := fm.load(name)
	if !ok {
		return false
	}

	e := v.(*fileEntryWithRWLock)
	e.Lock()
	defer e.Unlock()

	// Now that we have the entry lock, make sure k was not deleted or overwritten.
	if nv, ok := fm.load(name); !ok {
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
func (fm *lruFileMap) Delete(name string, f func(string, FileEntry) error) bool {
	v, ok := fm.load(name)
	if !ok {
		return false
	}

	e := v.(*fileEntryWithRWLock)
	e.Lock()
	defer e.Unlock()

	// Now that we have the entry lock, make sure k was not deleted or overwritten.
	if nv, ok := fm.load(name); !ok {
		return false
	} else if nv != v {
		return false
	}

	if err := f(name, e.fe); err != nil {
		return false
	}

	// Remove from map while the entry lock is still being held
	fm.Lock()
	fm.cache.Remove(name)
	fm.Unlock()

	return true
}
