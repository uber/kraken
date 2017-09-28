package store

import (
	"regexp"

	"code.uber.internal/infra/kraken/lib/store/base"
)

func init() {
	base.RegisterMetadata(regexp.MustCompile("_startedat"), &startedAtFactory{})
}

type startedAtFactory struct{}

func (f startedAtFactory) Create(suffix string) base.MetadataType {
	return NewStartedAt()
}

type startedAt struct{}

// NewStartedAt returns a new startedAt object.
// Registry reads the startedat file and removes uploads that have been active for too long.
func NewStartedAt() base.MetadataType {
	return &startedAt{}
}

func (s startedAt) GetSuffix() string {
	return "_startedat"
}

func (s startedAt) Movable() bool {
	return false
}
