package base

import (
	"regexp"
)

func init() {
	RegisterMetadata(regexp.MustCompile("_last_access_time"), &lastAccessTimeFactory{})
}

type lastAccessTimeFactory struct{}

func (f lastAccessTimeFactory) Create(suffix string) MetadataType {
	return NewLastAccessTime()
}

// lastAccessTime implements MetadataType. It's used to get file's last access time.
type lastAccessTime struct{}

// NewLastAccessTime initializes and returns an new MetadataType obj.
func NewLastAccessTime() MetadataType {
	return &lastAccessTime{}
}

func (t lastAccessTime) GetSuffix() string {
	return "_last_access_time"
}

func (t lastAccessTime) Movable() bool {
	return false
}
