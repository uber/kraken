package trackerserver

import (
	"time"

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
	config := Config{
		AnnounceInterval: 250 * time.Millisecond,
	}
	return New(config, tally.NoopScope, policy, storage.TestPeerStore(), nil, nil)
}
