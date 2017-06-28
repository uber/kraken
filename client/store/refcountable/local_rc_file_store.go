package refcountable

import "code.uber.internal/infra/kraken/client/store/base"

// LocalRCFileStore manages all agent files on local disk.
type LocalRCFileStore struct {
	*base.LocalFileStore
}

// NewLocalRCFileStore initializes and returns a new RCFileStore object.
func NewLocalRCFileStore(internalFactory base.FileEntryInternalFactory, factory base.FileEntryFactory) RCFileStore {
	return &LocalRCFileStore{
		LocalFileStore: base.NewLocalFileStore(internalFactory, factory).(*base.LocalFileStore),
	}
}

// IncrementFileRefCount increments file ref count. Ref count is stored in a metadata file on local disk.
func (s *LocalRCFileStore) IncrementFileRefCount(fileName string, states []base.FileState) (int64, error) {
	entry, v, err := s.LocalFileStore.LoadFileEntry(fileName, states)
	if err != nil {
		return 0, err
	}

	return entry.(RCFileEntry).IncrementRefCount(v)
}

// DecrementFileRefCount decrements file ref count. Ref count is stored in a metadata file on local disk.
func (s *LocalRCFileStore) DecrementFileRefCount(fileName string, states []base.FileState) (int64, error) {
	entry, v, err := s.LocalFileStore.LoadFileEntry(fileName, states)
	if err != nil {
		return 0, err
	}

	return entry.(RCFileEntry).DecrementRefCount(v)
}
