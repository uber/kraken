package torrent

import "code.uber.internal/infra/kraken/torlib"

// Spec specifies a new torrent for adding to a client.
type Spec struct {
	// The tiered tracker URIs.
	Trackers  [][]string
	InfoHash  torlib.InfoHash
	InfoBytes []byte
	// The name to use if the Name field from the Info isn't available.
	DisplayName string
}

// SpecFromtorlibInfo generates a torrent's spec by torlib info.
func SpecFromtorlibInfo(mi *torlib.MetaInfo) (*Spec, error) {
	infoBytes, err := mi.Info.Serialize()
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
		InfoBytes:   infoBytes,
		DisplayName: mi.Info.Name,
		InfoHash:    mi.GetInfoHash(),
	}, nil
}
