package store

import (
	"regexp"

	"code.uber.internal/infra/kraken/lib/store/internal"
)

func init() {
	internal.RegisterMetadata(regexp.MustCompile("_startedat"), &startedAtFactory{})
}

type startedAtFactory struct{}

func (f startedAtFactory) Create(suffix string) internal.MetadataType {
	return NewStartedAt()
}

type startedAt struct{}

// NewStartedAt returns a new startedAt object.
// Registry reads the startedat file and removes uploads that have been active for too long.
func NewStartedAt() internal.MetadataType {
	return &startedAt{}
}

func (s startedAt) GetSuffix() string {
	return "_startedat"
}

func (s startedAt) Movable() bool {
	return false
}
