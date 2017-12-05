package torlib

import "sort"

// AnnouncerResponse follows a bittorrent tracker protocol
// for tracker based peer discovery
type AnnouncerResponse struct {
	Interval int64      `bencode:"interval"`
	Peers    []PeerInfo `bencode:"peers"`
}

// PeerInfo defines metadata for a peer
type PeerInfo struct {
	InfoHash string `bencode:"info_hash"`
	PeerID   string `bencode:"peer_id"`
	IP       string `bencode:"ip"`
	Port     int64  `bencode:"port"`
	Priority int64  `bencode:"priority"`
	DC       string `bencode:"dc"`
	// TODO(codyg): Get rid of bencode because it can't encode boolean.
	Origin   bool `bencode:"-"`
	Complete bool `bencode:"-"`
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
