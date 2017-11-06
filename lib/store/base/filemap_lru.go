package base

import (
	"fmt"
	"sync"

	"code.uber.internal/go-common.git/x/log"

	"github.com/hashicorp/golang-lru/simplelru"
)

var _ FileMap = &lruFileMap{}

// LRUFileMapFactory creates a new lruFileMap
type LRUFileMapFactory struct {
	Size int
}

// Create returns a lruFileMap
func (lruFactory *LRUFileMapFactory) Create() (fileMap FileMap, err error) {
	return newlruFileMap(lruFactory.Size)
}

// lruFileMap implements FileMap interface
type lruFileMap struct {
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

// newlruFileMap creates a new lru map given size
func newlruFileMap(size int) (*lruFileMap, error) {
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

func (m *lruFileMap) Load(key interface{}) (value interface{}, ok bool) {
	m.Lock()
	defer m.Unlock()
	return m.cache.Get(key)
}

func (m *lruFileMap) LoadOrStore(key, value interface{}) (actual interface{}, loaded bool) {
	m.Lock()
	defer m.Unlock()
	v, ok := m.cache.Get(key)
	if ok {
		return v, true
	}

	m.cache.Add(key, value)
	return value, false
}

func (m *lruFileMap) Delete(key interface{}) {
	m.Lock()
	defer m.Unlock()
	m.cache.Remove(key)
}
