package peerhandoutpolicy

// DefaultPeerHandoutPolicyFixture returns the default peer handout policy.
func DefaultPeerHandoutPolicyFixture() PeerHandoutPolicy {
	p, err := Get("default", "default")
	if err != nil {
		panic(err)
	}
	return p
}
