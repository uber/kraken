package base

import (
	"regexp"
)

// Metadata defines types of matadata file.
// All implementations of Metadata must register themselves.
type Metadata interface {
	GetSuffix() string
	Movable() bool
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}

var _metadataFactory = make(map[*regexp.Regexp]MetadataFactory)

// MetadataFactory creates Metadata objects given suffix.
type MetadataFactory interface {
	Create(suffix string) Metadata
}

// RegisterMetadata registers new MetadataFactory with corresponding suffix regexp.
func RegisterMetadata(suffixRegexp *regexp.Regexp, factory MetadataFactory) {
	_metadataFactory[suffixRegexp] = factory
}

// CreateFromSuffix creates a Metadata obj based on suffix.
// This is not a very efficient method; It's mostly used during reload.
func CreateFromSuffix(suffix string) Metadata {
	for re, factory := range _metadataFactory {
		if re.MatchString(suffix) {
			return factory.Create(suffix)
		}
	}
	return nil
}
