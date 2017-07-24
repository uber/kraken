package utils

import (
	"log"
	"sync"

	"fmt"

	"golang.org/x/sync/syncmap"
)

// MapError wraps errors in LockerMap
type MapError struct {
	err string
}

func newMapError(err string) error {
	return &MapError{err: err}
}

func (e *MapError) Error() string {
	return fmt.Sprintf("LockerMap error: %s", e.err)
}

// LockerMap is a wrapper of golang's syncmap.Map
type LockerMap struct {
	m *syncmap.Map
}

// NewLockerMap creates a new map
func NewLockerMap() *LockerMap {
	return &LockerMap{
		m: new(syncmap.Map),
	}
}

// Load finds a value from map given key and exec do
func (lm *LockerMap) Load(key interface{}, do func(sync.Locker)) error {
	val, ok := lm.m.Load(key)
	if !ok {
		return newMapError("Unable to load. Key does not exist")
	}

	valLocker, ok := val.(sync.Locker)
	if !ok {
		log.Fatalf("Loaded invalid value type. Got %+v, Expected a sync.Locker", val)
	}

	valLocker.Lock()
	defer valLocker.Unlock()

	_, ok = lm.m.Load(key)
	if !ok {
		return newMapError("Unable to load. Key is deleted")
	}

	do(valLocker)
	return nil
}

// Store stores a new key, value pair in map and exec do
// if key already exists, return error
func (lm *LockerMap) Store(key interface{}, newLocker sync.Locker, do func(sync.Locker)) error {
	newLocker.Lock()
	defer newLocker.Unlock()

	_, loaded := lm.m.LoadOrStore(key, newLocker)
	// Key already exists in map, return error
	if loaded {
		return newMapError("Unable to store. Duplicated key")
	}

	do(newLocker)
	return nil
}

// Delete deletes key
func (lm *LockerMap) Delete(key interface{}) {
	val, ok := lm.m.Load(key)
	if !ok {
		return
	}

	valLocker, ok := val.(sync.Locker)
	if !ok {
		log.Fatalf("Loaded invalid value type. Got %+v, Expected a sync.Locker", val)
	}

	valLocker.Lock()
	defer valLocker.Unlock()

	_, ok = lm.m.Load(key)
	if !ok {
		return
	}

	lm.m.Delete(key)
}

// Range interates the map and exec do until do returns false
func (lm *LockerMap) Range(do func(val sync.Locker) bool) {
	lm.m.Range(func(key, val interface{}) bool {
		valLocker, ok := val.(sync.Locker)
		if !ok {
			log.Fatalf("Range on invalid value type. Got %+v, Expected a sync.Locker", val)
		}

		valLocker.Lock()
		defer valLocker.Unlock()

		_, ok = lm.m.Load(key)
		if !ok {
			// skip this entry because it is deleted
			return true
		}

		return do(valLocker)
	})
}
