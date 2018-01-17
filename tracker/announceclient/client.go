package announceclient

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
	"github.com/jackpal/bencode-go"
)

const _timeout = 30 * time.Second

// Client defines a client for announcing and getting peers.
type Client interface {
	Announce(name string, h torlib.InfoHash, complete bool) ([]torlib.PeerInfo, error)
}

// HTTPClient announces to tracker over HTTP.
type HTTPClient struct {
	pctx    peercontext.PeerContext
	servers serverset.Set
}

// New creates a new HTTPClient.
func New(pctx peercontext.PeerContext, servers serverset.Set) *HTTPClient {
	return &HTTPClient{pctx, servers}
}

// Announce announces the torrent identified by (name, h) with the number of
// downloaded bytes. Returns a list of all other peers announcing for said torrent,
// sorted by priority.
func (c *HTTPClient) Announce(name string, h torlib.InfoHash, complete bool) ([]torlib.PeerInfo, error) {
	v := url.Values{}

	v.Add("name", name)
	v.Add("info_hash", h.String())
	v.Add("peer_id", c.pctx.PeerID.String())
	v.Add("port", strconv.Itoa(c.pctx.Port))
	v.Add("ip", c.pctx.IP)
	v.Add("dc", c.pctx.Zone)
	v.Add("complete", strconv.FormatBool(complete))

	q := v.Encode()

	it := c.servers.Iter()
	for it.Next() {
		resp, err := httputil.Get(
			fmt.Sprintf("http://%s/announce?%s", it.Addr(), q),
			httputil.SendTimeout(_timeout))
		if err != nil {
			if _, ok := err.(httputil.NetworkError); ok {
				log.Errorf("Error announcing to %s: %s", it.Addr(), err)
				continue
			}
			return nil, err
		}
		var b struct {
			Peers []torlib.PeerInfo `bencode:"peers"`
		}
		if err := bencode.Unmarshal(resp.Body, &b); err != nil {
			return nil, fmt.Errorf("unmarshal failed: %s", err)
		}
		return b.Peers, nil
	}
	return nil, it.Err()
}

// DisabledClient rejects all announces. Suitable for origin peers which should
// not be announcing.
type DisabledClient struct{}

// Disabled returns a new DisabledClient.
func Disabled() Client {
	return DisabledClient{}
}

// Announce always returns error.
func (c DisabledClient) Announce(
	name string, h torlib.InfoHash, complete bool) ([]torlib.PeerInfo, error) {

	return nil, errors.New("announcing disabled")
}
