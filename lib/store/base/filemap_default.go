package base

import "golang.org/x/sync/syncmap"

var _ FileMap = &syncmap.Map{}

// DefaultFileMapFactory creates a new syncmap.Map as FileMap
type DefaultFileMapFactory struct{}

// Create returns a new syncmap.Map
func (DefaultFileMapFactory) Create() (fileMap FileMap, err error) {
	return &syncmap.Map{}, nil
}
