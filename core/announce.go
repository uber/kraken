package core

import "sort"

// AnnouncerResponse follows a bittorrent tracker protocol
// for tracker based peer discovery
type AnnouncerResponse struct {
	Peers []*PeerInfo `json:"peers"`
}

// PeerInfo defines metadata for a peer
type PeerInfo struct {
	InfoHash string `json:"info_hash"`
	PeerID   string `json:"peer_id"`
	IP       string `json:"ip"`
	Port     int64  `json:"port"`
	Priority int64  `json:"priority"`
	DC       string `json:"dc"`
	Origin   bool   `json:"origin"`
	Complete bool   `json:"complete"`
}

// SortedPeerIDs converts a list of peers into their peer ids in ascending order.
func SortedPeerIDs(peers []*PeerInfo) []string {
	pids := make([]string, len(peers))
	for i := range pids {
		pids[i] = peers[i].PeerID
	}
	sort.Strings(pids)
	return pids
}
