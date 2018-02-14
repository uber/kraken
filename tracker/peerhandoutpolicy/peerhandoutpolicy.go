package peerhandoutpolicy

import (
	"fmt"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/log"
)

// PeerPriorityPolicy defines the policy for assigning priority to peers.
type PeerPriorityPolicy interface {
	// AssignPeerPriority mutates peers by setting Priority for each PeerInfo
	// based on the source peer. Note, should not set Priority on source.
	AssignPeerPriority(source *core.PeerInfo, peers []*core.PeerInfo) error
}

// PeerSamplingPolicy defines the policy for selecting and ordering peers.
type PeerSamplingPolicy interface {
	// SamplePeers returns a new slice of n peers, sorted on sampling preference.
	SamplePeers(peers []*core.PeerInfo, n int) ([]*core.PeerInfo, error)
}

// PriorityFactory creates a PeerPriorityPolicy.
type PriorityFactory func() PeerPriorityPolicy

// SamplingFactory creates a PeerSamplingPolicy.
type SamplingFactory func() PeerSamplingPolicy

var (
	_priorityFactories = make(map[string]PriorityFactory)
	_samplingFactories = make(map[string]SamplingFactory)
)

func registerPriorityPolicy(name string, f PriorityFactory) {
	if f == nil {
		log.Panicf("No factory set for priority policy %s", name)
	}
	if _, ok := _priorityFactories[name]; ok {
		log.Errorf("Priority policy %s already registered, ignoring factory.", name)
	} else {
		_priorityFactories[name] = f
	}
}

func registerSamplingPolicy(name string, f SamplingFactory) {
	if f == nil {
		log.Panicf("No factory set for sampling policy %s", name)
	}
	if _, ok := _samplingFactories[name]; ok {
		log.Errorf("Sampling policy %s already registered, ignoring factory.", name)
	} else {
		_samplingFactories[name] = f
	}
}

func init() {
	registerPriorityPolicy("default", NewDefaultPeerPriorityPolicy)
	registerPriorityPolicy("ipv4netmask", NewIPv4NetmaskPeerPriorityPolicy)
	registerPriorityPolicy("mock", NewMockNetworkPriorityPolicy)

	registerSamplingPolicy("default", NewDefaultPeerSamplingPolicy)
	registerSamplingPolicy("completeness", NewCompletenessPeerSamplingPolicy)
}

// PeerHandoutPolicy composes priority and sampling policies. Aims to make priority
// of peers orthogonal to the suggested peers. The handout policy is thus broken down
// into two steps:
//     1. Assign priority to each peer.
//     2. Sample the top N peers.
type PeerHandoutPolicy struct {
	PeerPriorityPolicy
	PeerSamplingPolicy
}

// Get returns the registered PeerHandoutPolicy for the given priority and sampling policies.
func Get(priorityPolicy string, samplingPolicy string) (PeerHandoutPolicy, error) {
	var p PeerHandoutPolicy
	priorityFactory, ok := _priorityFactories[priorityPolicy]
	if !ok {
		return p, fmt.Errorf("priority policy %q not found", priorityPolicy)
	}
	samplingFactory, ok := _samplingFactories[samplingPolicy]
	if !ok {
		return p, fmt.Errorf("sampling policy %q not found", samplingPolicy)
	}
	p = PeerHandoutPolicy{
		PeerPriorityPolicy: priorityFactory(),
		PeerSamplingPolicy: samplingFactory(),
	}
	return p, nil
}
