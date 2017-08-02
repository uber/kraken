package service

import (
	"encoding/hex"
	"net/url"
	"strconv"

	"code.uber.internal/infra/kraken/tracker/storage"
)

func createAnnouncePath(t *storage.TorrentInfo, p *storage.PeerInfo) string {
	rawInfoHash, err := hex.DecodeString(t.InfoHash)
	if err != nil {
		panic(err)
	}

	v := url.Values{}
	v.Set("info_hash", string(rawInfoHash))
	v.Set("peer_id", p.PeerID)
	v.Set("ip", p.IP)
	v.Set("port", strconv.FormatInt(p.Port, 10))
	v.Set("dc", p.DC)
	v.Set("downloaded", strconv.FormatInt(p.BytesDownloaded, 10))
	v.Set("uploaded", strconv.FormatInt(p.BytesUploaded, 10))
	v.Set("left", strconv.FormatInt(p.BytesLeft, 10))
	v.Set("event", p.Event)

	return "/announce?" + v.Encode()
}
