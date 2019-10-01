package conn

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/log"

	"github.com/willf/bitset"
)

// FakePeer is a testing utility which reciprocates handshakes against
// arbitrary incoming connections, parroting back the requested torrent but
// with an empty bitfield (so no pieces are requested).
//
// Useful for initializing real Conns against a motionless peer.
type FakePeer struct {
	listener net.Listener

	id   core.PeerID
	ip   string
	port int

	msgTimeout time.Duration
}

// NewFakePeer creates and starts a new FakePeer.
func NewFakePeer() (*FakePeer, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	ip, portStr, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	p := &FakePeer{
		listener:   l,
		id:         core.PeerIDFixture(),
		ip:         ip,
		port:       port,
		msgTimeout: 5 * time.Second,
	}
	go func() {
		err := p.handshakeConns()
		log.Infof("Fake peer exiting: %s", err)
	}()
	return p, nil
}

// PeerID returns the peer's PeerID.
func (p *FakePeer) PeerID() core.PeerID {
	return p.id
}

// Addr returns the ip:port of the peer.
func (p *FakePeer) Addr() string {
	return fmt.Sprintf("%s:%d", p.ip, p.port)
}

// PeerInfo returns the peers' PeerInfo.
func (p *FakePeer) PeerInfo() *core.PeerInfo {
	return core.NewPeerInfo(p.id, p.ip, p.port, false, false)
}

// Close shuts down the peer.
func (p *FakePeer) Close() {
	p.listener.Close()
}

func (p *FakePeer) handshakeConns() error {
	for {
		nc, err := p.listener.Accept()
		if err != nil {
			return err
		}
		reqMsg, err := readMessageWithTimeout(nc, p.msgTimeout)
		if err != nil {
			return err
		}
		req, err := handshakeFromP2PMessage(reqMsg)
		if err != nil {
			return err
		}
		resp := &handshake{
			peerID:   p.id,
			digest:   req.digest,
			infoHash: req.infoHash,
			// Oh darn, we have no pieces!
			bitfield:  bitset.New(req.bitfield.Len()),
			namespace: req.namespace,
		}
		respMsg, err := resp.toP2PMessage()
		if err != nil {
			return err
		}
		if err := sendMessageWithTimeout(nc, respMsg, p.msgTimeout); err != nil {
			return err
		}
	}
}
