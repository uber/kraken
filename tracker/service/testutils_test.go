package service

import (
	"encoding/hex"
	"net"
	"net/url"
	"strconv"

	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils"
)

func createAnnouncePath(t *storage.TorrentInfo, p *storage.PeerInfo) string {
	rawInfoHash, err := hex.DecodeString(t.InfoHash)
	if err != nil {
		panic(err)
	}
	rawPeerID, err := hex.DecodeString(p.PeerID)
	if err != nil {
		panic(err)
	}

	v := url.Values{}
	v.Set("info_hash", string(rawInfoHash))
	v.Set("peer_id", string(rawPeerID))
	v.Set("ip", strconv.Itoa(int(utils.IPtoInt32(net.ParseIP(p.IP)))))
	v.Set("port", strconv.FormatInt(p.Port, 10))
	v.Set("dc", p.DC)
	v.Set("downloaded", strconv.FormatInt(p.BytesDownloaded, 10))
	v.Set("uploaded", strconv.FormatInt(p.BytesUploaded, 10))
	v.Set("left", strconv.FormatInt(p.BytesLeft, 10))
	v.Set("event", p.Event)

	return "/announce?" + v.Encode()
}
