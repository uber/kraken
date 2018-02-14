package core

import "errors"

// PeerContext defines the context a peer runs within, namely the fields which
// are used to identify each peer.
type PeerContext struct {

	// IP and Port specify the address the peer will announce itself as. Note,
	// this is distinct from the address a peer's Scheduler will listen on
	// because the peer may be running within a container and the address it
	// listens on is mapped to a different ip/port outside of the container.
	IP   string `json:"ip"`
	Port int    `json:"port"`

	// PeerID the peer will identify itself as.
	PeerID PeerID `json:"peer_id"`

	// Zone is the zone the peer is running within.
	Zone string `json:"zone"`

	// Origin indicates whether the peer is an origin server or not.
	Origin bool `json:"origin"`
}

// NewPeerContext creates a new PeerContext.
func NewPeerContext(
	f PeerIDFactory, zone string, ip string, port int, origin bool) (PeerContext, error) {

	if ip == "" {
		return PeerContext{}, errors.New("no ip supplied")
	}
	if port == 0 {
		return PeerContext{}, errors.New("no port supplied")
	}
	peerID, err := f.GeneratePeerID(ip, port)
	if err != nil {
		return PeerContext{}, err
	}
	return PeerContext{
		IP:     ip,
		Port:   port,
		PeerID: peerID,
		Zone:   zone,
		Origin: origin,
	}, nil
}
