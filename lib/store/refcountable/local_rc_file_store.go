package refcountable

import "code.uber.internal/infra/kraken/lib/store/base"

// LocalRCFileStore manages all agent files on local disk.
type LocalRCFileStore struct {
	*base.LocalFileStore
}

// NewLocalRCFileStore initializes and returns a new RCFileStore object.
// It allows dependency injection.
func NewLocalRCFileStore(
	fileEntryInternalFactory base.FileEntryInternalFactory,
	fileEntryFactory base.FileEntryFactory,
	fileMapFactory base.FileMapFactory) (RCFileStore, error) {
	store, err := base.NewLocalFileStore(fileEntryInternalFactory, fileEntryFactory, fileMapFactory)
	if err != nil {
		return nil, err
	}
	return &LocalRCFileStore{
		LocalFileStore: store,
	}, nil
}

// NewLocalRCFileStoreDefault initializes and returns a new RCFileStore object.
func NewLocalRCFileStoreDefault() (RCFileStore, error) {
	store, err := NewLocalRCFileStore(NewLocalRCFileEntryInternalFactory(), &LocalRCFileEntryFactory{}, &base.DefaultFileMapFactory{})
	if err != nil {
		return nil, err
	}
	return store, nil
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
