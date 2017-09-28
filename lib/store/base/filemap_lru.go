package base

import (
	"fmt"
	"sync"

	"code.uber.internal/go-common.git/x/log"

	"github.com/hashicorp/golang-lru/simplelru"
)

var _ FileMap = &lruMap{}

// LRUFileMapFactory creates a new lruMap
type LRUFileMapFactory struct {
	size int
}

// Create returns a lruMap
func (lruFactory *LRUFileMapFactory) Create() (fileMap FileMap, err error) {
	return newlruMap(lruFactory.size)
}

// lruMap implements FileMap interface
type lruMap struct {
	sync.Mutex

	cache *simplelru.LRU
}

func onEvictCallBack(key interface{}, value interface{}) {
	fe := value.(*LocalFileEntry)
	// entry will be removed from the map, so we do not need to verify
	if err := fe.Delete(func(FileEntry) error { return nil }); err != nil {
		log.Errorf("unable to delete file entry on cache eviction %s", err)
	}
}

// newlruMap creates a new lru map given size
func newlruMap(size int) (*lruMap, error) {
	if size <= 0 {
		return nil, fmt.Errorf("Invalid map size: %d", size)
	}

	c, err := simplelru.NewLRU(size, onEvictCallBack)
	if err != nil {
		return nil, err
	}

	m := &lruMap{
		cache: c,
	}

	return m, nil
}

func (m *lruMap) Load(key interface{}) (value interface{}, ok bool) {
	m.Lock()
	defer m.Unlock()
	return m.cache.Get(key)
}

func (m *lruMap) LoadOrStore(key, value interface{}) (actual interface{}, loaded bool) {
	m.Lock()
	defer m.Unlock()
	v, ok := m.cache.Get(key)
	if ok {
		return v, true
	}

	m.cache.Add(key, value)
	return value, false
}

func (m *lruMap) Delete(key interface{}) {
	m.Lock()
	defer m.Unlock()
	m.cache.Remove(key)
}
