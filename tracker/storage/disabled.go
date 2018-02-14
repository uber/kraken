package storage

import "errors"

var errManifestStoreDisabled = errors.New("manifest store disabled")

type disabled struct{}

// GetManifest always returns error.
func (d disabled) GetManifest(tag string) (string, error) {
	return "", errManifestStoreDisabled
}

// CreateManifest always returns error.
func (d disabled) CreateManifest(tag, manifestRaw string) error {
	return errManifestStoreDisabled
}

// DeleteManifest always returns error
func (d disabled) DeleteManifest(tag string) error {
	return errManifestStoreDisabled
}
