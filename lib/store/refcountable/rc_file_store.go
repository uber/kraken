package refcountable

import "code.uber.internal/infra/kraken/lib/store/base"

// RCFileEntryInternal extends base.FileEntryInternal, adds function to manage file ref count.
type RCFileEntryInternal interface {
	base.FileEntryInternal

	GetRefCount() (int64, error)
	IncrementRefCount() (int64, error)
	DecrementRefCount() (int64, error)
}

// RCFileEntry extends base.FileEntry, adds function to manage file ref count in a thread safe manner.
type RCFileEntry interface {
	base.FileEntry

	GetRefCount(v base.Verify) (int64, error)
	IncrementRefCount(v base.Verify) (int64, error)
	DecrementRefCount(v base.Verify) (int64, error)
}

// RCFileStore extends base.FileStore, adds ref count for files.
type RCFileStore interface {
	base.FileStore

	IncrementFileRefCount(fileName string, states []base.FileState) (int64, error)
	DecrementFileRefCount(fileName string, states []base.FileState) (int64, error)
}
