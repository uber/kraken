package base

import "github.com/andres-erbsen/clock"

// localRCFileStore is an extension of localFileStore, that also keeps file ref count on disk.
type localRCFileStore struct {
	*localFileStore
}

// NewFileOp inits a new RCFileOp.
func (store localRCFileStore) NewFileOp() FileOp {
	op := store.localFileStore.NewFileOp().(*localFileOp)
	return NewLocalRCFileOp(op)
}

// NewLocalRCFileStore inits a new localRCFileStore.
func NewLocalRCFileStore() (FileStore, error) {
	store, err := NewCASFileStore(clock.New())
	if err != nil {
		return nil, err
	}
	return &localRCFileStore{
		localFileStore: store.(*localFileStore),
	}, nil
}
