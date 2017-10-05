package peercontext

import (
	"errors"
	"fmt"

	xconfig "code.uber.internal/go-common.git/x/config"

	"code.uber.internal/infra/kraken/torlib"
)

// PeerIDFactory defines the method used to generate a peer id.
type PeerIDFactory string

// RandomPeerIDFactory creates random peer ids.
const RandomPeerIDFactory PeerIDFactory = "random"

// IPHashPeerIDFactory creates peer ids based on a hash of the peers ip address.
const IPHashPeerIDFactory PeerIDFactory = "ip_hash"

// GeneratePeerID creates a new peer id per the factory policy.
func (f PeerIDFactory) GeneratePeerID(ip string) (torlib.PeerID, error) {
	switch f {
	case RandomPeerIDFactory:
		return torlib.RandomPeerID()
	case IPHashPeerIDFactory:
		return torlib.HashedPeerID(ip)
	default:
		err := fmt.Errorf("invalid peer id factory: %q", string(f))
		return torlib.PeerID{}, err
	}
}

// PeerContext defines the context a peer runs within, namely the fields which
// are used to identify each peer.
type PeerContext struct {

	// IP and Port specify the address the peer will announce itself as. Note,
	// this is distinct from the address a peer's Scheduler will listen on
	// because the peer may be running within a container and the address it
	// listens on is mapped to a different ip/port outside of the container.
	IP   string
	Port int

	// PeerID the peer will identify itself as.
	PeerID torlib.PeerID

	// Zone is the zone the peer is running within.
	Zone string
}

// New creates a new PeerContext.
func New(f PeerIDFactory, ip string, port int) (PeerContext, error) {
	if ip == "" {
		return PeerContext{}, errors.New("no ip supplied")
	}
	if port == 0 {
		return PeerContext{}, errors.New("no port supplied")
	}
	peerID, err := f.GeneratePeerID(ip)
	if err != nil {
		return PeerContext{}, err
	}
	return PeerContext{
		IP:     ip,
		Port:   port,
		PeerID: peerID,
		Zone:   xconfig.GetZone(),
	}, nil
}
