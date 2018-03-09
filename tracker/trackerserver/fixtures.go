package trackerserver

import (
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/storage"
)

// Fixture is a test utility which returns a tracker server with in-memory storage.
func Fixture() *Server {
	policy, err := peerhandoutpolicy.Get("ipv4netmask", "completeness")
	if err != nil {
		panic(err)
	}
	return New(Config{}, tally.NoopScope, policy, storage.TestPeerStore(), nil, nil, nil)
}
