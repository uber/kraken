package internal

import (
	"regexp"
)

func init() {
	RegisterMetadata(regexp.MustCompile("_refcount"), &refCountFactory{})
}

type refCountFactory struct{}

func (f refCountFactory) Create(suffix string) MetadataType {
	return NewRefCount()
}

// refCount implements MetadataType. It's used to manage file ref count.
type refCount struct{}

// NewRefCount initializes and returns an new MetadataType obj.
func NewRefCount() MetadataType {
	return &refCount{}
}

func (r refCount) GetSuffix() string {
	return "_refcount"
}

func (r refCount) Movable() bool {
	return false
}
