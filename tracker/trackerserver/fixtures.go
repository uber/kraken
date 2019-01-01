package trackerserver

import (
	"time"

	"github.com/uber-go/tally"

	"github.com/uber/kraken/tracker/originstore"
	"github.com/uber/kraken/tracker/peerhandoutpolicy"
	"github.com/uber/kraken/tracker/peerstore"
)

// Fixture is a test utility which returns a tracker server with in-memory storage.
func Fixture() *Server {
	policy := peerhandoutpolicy.DefaultPriorityPolicyFixture()
	config := Config{
		AnnounceInterval: 250 * time.Millisecond,
	}
	return New(
		config, tally.NoopScope, policy,
		peerstore.NewTestStore(), originstore.NewNoopStore(), nil)
}
