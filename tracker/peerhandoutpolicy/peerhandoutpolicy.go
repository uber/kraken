package peerhandoutpolicy

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/tracker/storage"
)

// PeerPriorityPolicy defines the policy for assigning priority to peers.
type PeerPriorityPolicy interface {
	// AssignPeerPriority mutates peers by setting Priority for each PeerInfo
	// based on the source peer. Note, should not set Priority on source.
	AssignPeerPriority(source *storage.PeerInfo, peers []*storage.PeerInfo) error
}

// PeerSamplingPolicy defines the policy for selecting and ordering peers.
type PeerSamplingPolicy interface {
	// SamplePeers returns a new slice of n peers, sorted on sampling preference.
	SamplePeers(peers []*storage.PeerInfo, n int) ([]*storage.PeerInfo, error)
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
// Second return value is false if policy does not exist.
func Get(priorityPolicy string, samplingPolicy string) (PeerHandoutPolicy, bool) {
	priorityFactory, ok := _priorityFactories[priorityPolicy]
	if !ok {
		return PeerHandoutPolicy{}, false
	}
	samplingFactory, ok := _samplingFactories[samplingPolicy]
	if !ok {
		return PeerHandoutPolicy{}, false
	}
	return PeerHandoutPolicy{
		PeerPriorityPolicy: priorityFactory(),
		PeerSamplingPolicy: samplingFactory(),
	}, true
}
