package peerhandoutpolicy

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/tracker/storage"
)

// PeerHandoutPolicy is an abstract interface for specific
// policies handout implementations
type PeerHandoutPolicy interface {
	GetPeers(sourceIP string, sourceDC string, peers []storage.PeerInfo) ([]storage.PeerInfo, error)
}

// Factory an auxiliary type for policy factories
type Factory func() PeerHandoutPolicy

// PeerHandoutPolicies the actual policies
var PeerHandoutPolicies = make(map[string]Factory)

// Register registers a name and a factory in a system
func Register(name string, policyFactory Factory) {
	if policyFactory == nil {
		log.Panicf("peer handout policy factory %s does not exist.", name)
	}
	_, registered := PeerHandoutPolicies[name]
	if registered {
		log.Error("peer handout policy %s already registered. Ignoring %s.", name)
	}
	PeerHandoutPolicies[name] = policyFactory
}

// NewIPv4PeerHandoutPolicy creates Uber specific policy for network
// proximity based peer handout
func NewIPv4PeerHandoutPolicy() PeerHandoutPolicy {
	return &IPv4NetmaskPeerHandoutPolicy{}
}

// NewDefaultPeerHandoutPolicy is a NO-OP policy
func NewDefaultPeerHandoutPolicy() PeerHandoutPolicy {
	return &DefaultPeerHandoutPolicy{}
}

// Init registers all policies in a system
func init() {
	Register("default", NewDefaultPeerHandoutPolicy)
	Register("ipv4netmask", NewIPv4PeerHandoutPolicy)
}
