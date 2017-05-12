package peerhandoutpolicy

import (
	"os"

	"code.uber.internal/infra/kraken/tracker/storage"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	peers []storage.PeerInfo
)

func TestMain(m *testing.M) {
	//TODO: turn it into a proper fixrture object,
	infoHash := "12345678901234567890"
	peerID := "09876543210987654321"

	peers = []storage.PeerInfo{
		{
			InfoHash: infoHash,
			PeerID:   peerID,
			IP:       "10.12.14.16", //same rack
			DC:       "sjc1",
		},
		{
			InfoHash: infoHash,
			PeerID:   peerID,
			IP:       "10.12.127.1", //same pod
			DC:       "sjc1",
		},
		{
			InfoHash: infoHash,
			PeerID:   peerID,
			IP:       "10.15.0.1", //same datacenter
			DC:       "sjc1",
		},
		{
			InfoHash: infoHash,
			PeerID:   peerID,
			IP:       "10.17.22.28", // different datacenter
			DC:       "dca1",
		},
	}
	os.Exit(m.Run())
}

func TestDefaultHandoutPolicy(t *testing.T) {

	t.Run("Sort peers by priority by racks, pods and datacenters affinity", func(t *testing.T) {
		sortedPeers, err := PeerHandoutPolicies["default"]().GetPeers("10.12.14.15", "sjc1", peers)
		assert.Equal(t, err, nil)
		for _, peer := range sortedPeers {
			assert.NotEqual(t, peer.Priority, 0)
		}
	})
}

func TestIPv4NetmaskHandoutPolicy(t *testing.T) {

	t.Run("Sort peers by priority by racks, pods and datacenters affinity", func(t *testing.T) {
		sortedPeers, err := PeerHandoutPolicies["ipv4netmask"]().GetPeers("10.12.14.15", "sjc1", peers)
		assert.Equal(t, err, nil)
		t.Log(sortedPeers)
		for index, peer := range sortedPeers {
			assert.Equal(t, peer.Priority, int64(index))
		}
	})

}
