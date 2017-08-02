package torrent

import "code.uber.internal/infra/kraken/client/torrent/meta"

// Spec specifies a new torrent for adding to a client.
type Spec struct {
	// The tiered tracker URIs.
	Trackers  [][]string
	InfoHash  meta.Hash
	InfoBytes []byte
	// The name to use if the Name field from the Info isn't available.
	DisplayName string
}

// SpecFromMetaInfo generates a torrent's spec by meta info.
func SpecFromMetaInfo(mi *meta.TorrentInfo) (*Spec, error) {
	info, err := mi.UnmarshalInfo()
	if err != nil {
		return nil, err
	}
	var trackers [][]string
	if mi.AnnounceList != nil {
		trackers = mi.AnnounceList
	} else if mi.Announce != "" {
		trackers = [][]string{{mi.Announce}}
	}
	return &Spec{
		Trackers:    trackers,
		InfoBytes:   mi.InfoBytes,
		DisplayName: info.Name,
		InfoHash:    mi.HashInfoBytes(),
	}, nil
}
