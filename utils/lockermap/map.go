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
package lockermap

import (
	"sync"

	"golang.org/x/sync/syncmap"
)

// Map is a two-level locking map which synchronizes access to the map
// in addition to synchronizing access to the values within the map. Useful
// for mutating values in-place.
//
// The zero Map is valid and empty.
type Map struct {
	m syncmap.Map
}

// Load looks up the value of key k and executes f under the protection of
// said value's lock. While f executes, it is guaranteed that k will not
// be deleted from the map. Returns false if k was not found.
func (m *Map) Load(k interface{}, f func(sync.Locker)) bool {
	v, ok := m.m.Load(k)
	if !ok {
		return false
	}

	l := v.(sync.Locker)
	l.Lock()
	defer l.Unlock()

	// Now that we have the value lock, make sure k was not deleted.
	if _, ok = m.m.Load(k); !ok {
		return false
	}

	f(l)

	return true
}

// TryStore stores the given key / value pair within the Map. Returns
// false if the key is already present.
func (m *Map) TryStore(k interface{}, v sync.Locker) bool {
	_, loaded := m.m.LoadOrStore(k, v)
	return !loaded
}

// Delete deletes the given key from the Map.
func (m *Map) Delete(k interface{}) {
	v, ok := m.m.Load(k)
	if !ok {
		return
	}

	l := v.(sync.Locker)
	l.Lock()
	defer l.Unlock()

	m.m.Delete(k)
}

// Range interates over the Map and execs f until f returns false.
func (m *Map) Range(f func(k interface{}, v sync.Locker) bool) {
	m.m.Range(func(k, v interface{}) bool {
		l := v.(sync.Locker)
		l.Lock()
		defer l.Unlock()

		if _, ok := m.m.Load(k); !ok {
			// Skip this entry because it has been deleted.
			return true
		}

		return f(k, l)
	})
}
