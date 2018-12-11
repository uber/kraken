package originstore

import "github.com/uber/kraken/core"

type noopStore struct{}

// NoopStore returns a Store which never returns origins. Useful for testing.
func NoopStore() Store {
	return noopStore{}
}

func (s noopStore) GetOrigins(core.Digest) ([]*core.PeerInfo, error) {
	return nil, nil
}
