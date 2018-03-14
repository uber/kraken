package peerhandoutpolicy

import (
	"testing"

	"code.uber.internal/infra/kraken/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixture struct {
	ip string
	dc string
}

const (
	sourceIP = "10.12.14.15"
	sourceDC = "sjc1"
)

var (
	sameRack       = fixture{"10.12.14.16", "sjc1"}
	samePod        = fixture{"10.12.127.1", "sjc1"}
	sameDatacenter = fixture{"10.15.0.1", "sjc1"}
	diffDatacenter = fixture{"10.17.22.28", "dca1"}
)

func makeSourcePeer() *core.PeerInfo {
	return &core.PeerInfo{
		IP: sourceIP,
		DC: sourceDC,
	}
}

func makePeerFixtures(fs ...fixture) []*core.PeerInfo {
	var peers []*core.PeerInfo
	for _, fixture := range fs {
		p := &core.PeerInfo{}
		p.IP = fixture.ip
		p.DC = fixture.dc
		peers = append(peers, p)
	}

	// Sanity check to make sure we're not creating duplicate peers.
	ips := make(map[string]bool)
	for _, p := range peers {
		if ips[p.IP] {
			panic("duplicate IPs in fixtures")
		}
		ips[p.IP] = true
	}

	return peers
}

func TestDefaultPeerPriorityPolicy(t *testing.T) {

	t.Run("Assign priority 0 to all", func(t *testing.T) {
		policy, _ := Get("default", "default")
		src := makeSourcePeer()
		peers := makePeerFixtures(sameRack, samePod, sameDatacenter)
		for _, p := range peers {
			// Initialize priority to non-zero to make sure we actually set something.
			p.Priority = 5
		}
		require.NoError(t, policy.AssignPeerPriority(src, peers))
		for _, peer := range peers {
			assert.Equal(t, 0, peer.Priority)
		}
	})
}

func TestIPv4NetmaskPeerPriorityPolicy(t *testing.T) {

	t.Run("Prioritize by racks, pods and datacenters affinity", func(t *testing.T) {
		policy, _ := Get("ipv4netmask", "default")
		src := makeSourcePeer()
		peers := makePeerFixtures(sameRack, samePod, sameDatacenter, diffDatacenter)
		require.NoError(t, policy.AssignPeerPriority(src, peers))
		expected := map[fixture]int{
			sameRack:       0,
			samePod:        1,
			sameDatacenter: 2,
			diffDatacenter: 3,
		}
		assert.Equal(t, len(expected), len(peers))
		for fixture, priority := range expected {
			for _, p := range peers {
				if p.IP == fixture.ip {
					assert.Equal(t, priority, p.Priority)
					break
				}
			}
		}
	})
}

func TestDefaultPeerSamplingPolicy(t *testing.T) {

	t.Run("Sorts peers by priority", func(t *testing.T) {
		policy, _ := Get("ipv4netmask", "default")
		src := makeSourcePeer()
		peers := makePeerFixtures(sameRack, samePod, sameDatacenter)
		require.NoError(t, policy.AssignPeerPriority(src, peers))
		peers, err := policy.SamplePeers(peers, len(peers))
		require.NoError(t, err)
		for i, priority := range []int{0, 1, 2} {
			assert.Equal(t, priority, peers[i].Priority)
		}
	})
}
