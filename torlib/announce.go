package torlib

import (
	"net/http"
	"net/url"
	"strconv"
)

// AnnouncerResponse follows a bittorrent tracker protocol
// for tracker based peer discovery
type AnnouncerResponse struct {
	Interval int64      `bencode:"interval"`
	Peers    []PeerInfo `bencode:"peers"`
}

// PeerInfo defines metadata for a peer
type PeerInfo struct {
	InfoHash string `bencode:"info_hash" db:"infoHash"`
	PeerID   string `bencode:"peer_id" db:"peerId"`
	IP       string `bencode:"ip" db:"ip"`
	Port     int64  `bencode:"port" db:"port"`
	Priority int64  `bencode:"priority" db:"-"`

	DC              string `bencode:"dc,omitempty" db:"dc"`
	BytesDownloaded int64  `bencode:"downloaded,omitempty" db:"bytes_downloaded"`

	BytesUploaded int64  `bencode:"-" db:"-"`
	BytesLeft     int64  `bencode:"-" db:"-"`
	Event         string `bencode:"-" db:"-"`
	Flags         uint   `bencode:"-" db:"flags"`
}

// NewAnnounceRequest creates a new announce request given tracker location, and peerInfo
func NewAnnounceRequest(host, scheme, path string, p PeerInfo) *http.Request {
	v := url.Values{}

	v.Add("info_hash", p.InfoHash)
	v.Add("peer_id", p.PeerID)
	v.Add("port", strconv.FormatInt(p.Port, 10))
	v.Add("ip", p.IP)
	v.Add("dc", p.DC)
	v.Add("downloaded", strconv.FormatInt(p.BytesDownloaded, 10))

	return &http.Request{
		Method: "GET",
		URL: &url.URL{
			Host:     host,
			Scheme:   scheme,
			Path:     path,
			RawQuery: v.Encode(),
		},
	}
}
