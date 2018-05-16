package base

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"time"
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
	return true
}

// MarshalLastAccessTime marshals time to bytes.
func MarshalLastAccessTime(t time.Time) []byte {
	b := make([]byte, 8)
	binary.PutVarint(b, t.Unix())
	return b
}

// UnmarshalLastAccessTime unmarshals time from bytes.
func UnmarshalLastAccessTime(b []byte) (time.Time, error) {
	i, n := binary.Varint(b)
	if n <= 0 {
		return time.Time{}, fmt.Errorf("unmarshal last access time: %s", b)
	}
	return time.Unix(int64(i), 0), nil
}
