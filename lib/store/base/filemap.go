package base

import "golang.org/x/sync/syncmap"

// FileMapFactory is an interface initilizes a FileMap object
type FileMapFactory interface {
	Create() (fileMap FileMap, error error)
}

// FileMap is an interface contains
type FileMap interface {
	Load(key interface{}) (value interface{}, ok bool)
	LoadOrStore(key, value interface{}) (actual interface{}, loaded bool)
	Delete(key interface{})
}

var _ FileMap = &syncmap.Map{}

// DefaultFileMapFactory creates a new syncmap.Map as FileMap
type DefaultFileMapFactory struct{}

// Create returns a new syncmap.Map
func (DefaultFileMapFactory) Create() (fileMap FileMap, err error) {
	return &syncmap.Map{}, nil
}
