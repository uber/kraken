package trackerserver

import (
	"time"

	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/tracker/originstore"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/peerstore"
)

// Fixture is a test utility which returns a tracker server with in-memory storage.
func Fixture() *Server {
	policy := peerhandoutpolicy.DefaultPriorityPolicyFixture()
	config := Config{
		AnnounceInterval: 250 * time.Millisecond,
	}
	return New(config, tally.NoopScope, policy, peerstore.TestStore(), originstore.NoopStore(), nil)
}
