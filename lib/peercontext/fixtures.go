package peercontext

import (
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/randutil"
)

// Fixture returns a randomly generated PeerContext.
func Fixture() PeerContext {
	return PeerContext{
		IP:     randutil.IP(),
		Port:   randutil.Port(),
		PeerID: torlib.PeerIDFixture(),
		Zone:   "sjc1",
	}
}
