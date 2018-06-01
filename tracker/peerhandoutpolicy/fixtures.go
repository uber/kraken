package peerhandoutpolicy

import "github.com/uber-go/tally"

// DefaultPriorityPolicyFixture returns the default peer handout policy for testing purposes.
func DefaultPriorityPolicyFixture() *PriorityPolicy {
	p, err := NewPriorityPolicy(tally.NoopScope, "default")
	if err != nil {
		panic(err)
	}
	return p
}
