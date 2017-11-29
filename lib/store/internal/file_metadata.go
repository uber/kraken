package internal

import (
	"regexp"
)

// MetadataType defines types of matadata file.
// All implementations of MetadataType must register itself.
// TODO: instead of using suffix to identify MetadataType on local disk, move data file to
// <filename>/data, metadata to <filename>/<metadata>
type MetadataType interface {
	GetSuffix() string
	Movable() bool
}

var _metadataFactory = make(map[*regexp.Regexp]MetadataFactory)

// MetadataFactory creates MetadataType objects given suffix.
type MetadataFactory interface {
	Create(suffix string) MetadataType
}

// RegisterMetadata registers new MetadataFactory with corresponding suffix regexp.
func RegisterMetadata(suffixRegexp *regexp.Regexp, factory MetadataFactory) {
	_metadataFactory[suffixRegexp] = factory
}

// CreateFromSuffix creates a MetadataType obj based on suffix.
// This is not a very efficient method; It's mostly used during reload.
func CreateFromSuffix(suffix string) MetadataType {
	for re, factory := range _metadataFactory {
		if re.MatchString(suffix) {
			return factory.Create(suffix)
		}
	}
	return nil
}
