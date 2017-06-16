package peerhandoutpolicy

import (
	"errors"
	"strings"

	"code.uber.internal/infra/kraken/tracker/storage"
)

// ErrInvalidPeerIDFormat indicates an invalid peer id format.
var ErrInvalidPeerIDFormat = errors.New("peer id must be of format <id>:<rack>:<pod>:<dc>")

type mockLocation struct {
	rack string
	pod  string
	dc   string
}

func parseMockLocation(peerID string) (mockLocation, error) {
	parts := strings.Split(peerID, ":")
	if len(parts) != 4 {
		return mockLocation{}, ErrInvalidPeerIDFormat
	}
	return mockLocation{
		rack: parts[1],
		pod:  parts[2],
		dc:   parts[3],
	}, nil
}

// MockNetworkPriorityPolicy determines peer priority based on a special
// peer id format, "<id>:<rack>:<pod>:<dc>". This allows peers to declare
// their location in a mock network topology. Rack, pod, and dc may be
// arbitrary string identifiers. Note, this is a complete abuse of peer
// ids and is intended for integration testing only.
type MockNetworkPriorityPolicy struct{}

// NewMockNetworkPriorityPolicy is factory for PeerPriorityPolicy.
func NewMockNetworkPriorityPolicy() PeerPriorityPolicy {
	return &MockNetworkPriorityPolicy{}
}

// AssignPeerPriority sets priority based on the mock network topology
// of the peers, as described by their peer ids.
func (p *MockNetworkPriorityPolicy) AssignPeerPriority(
	source *storage.PeerInfo, peers []*storage.PeerInfo) error {

	src, err := parseMockLocation(source.PeerID)
	if err != nil {
		return err
	}

	// Make sure we can parse all peer ids before we mutate any peers.
	dests := make([]mockLocation, len(peers))
	for i, peer := range peers {
		l, err := parseMockLocation(peer.PeerID)
		if err != nil {
			return err
		}
		dests[i] = l
	}

	for i, peer := range peers {
		dest := dests[i]

		predicates := []bool{
			src.rack == dest.rack,
			src.pod == dest.pod,
			src.dc == dest.dc,
		}
		peer.Priority = calcPriority(predicates)
	}

	return nil
}
