package store

import "errors"

// BlobScope is the set of blobs that a store's APIs can operate on.
type BlobScope int

// Flags to scope a store's APIs to a subset of blobs.
const (
	BlobScopeAny BlobScope = iota
	BlobScopeComplete
	BlobScopeIncomplete
)

// ErrOutOfScope is returned when the provided key is in the store, but not in the store's [BlobScope],
// e.g. if the blob is incomplete, but ScopeComplete was called.
var ErrOutOfScope = errors.New("the blob is in store but filtered by the selected scope")
