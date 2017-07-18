package refcountable

import (
	"regexp"

	"code.uber.internal/infra/kraken/client/store/base"
)

func init() {
	base.RegisterMetadata(regexp.MustCompile("_refcount"), &refCountFactory{})
}

type refCountFactory struct{}

func (f refCountFactory) Create(suffix string) base.MetadataType {
	return NewRefCount()
}

// refCount implements MetadataType. It's used to manage file ref count.
type refCount struct{}

// NewRefCount initializes and returns an new MetadataType obj.
func NewRefCount() base.MetadataType {
	return &refCount{}
}

func (r refCount) GetSuffix() string {
	return "_refcount"
}

func (r refCount) Movable() bool {
	return false
}
