package announceclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/healthcheck"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Request defines an announce request.
type Request struct {
	Name     string         `json:"name"`
	InfoHash core.InfoHash  `json:"info_hash"`
	Peer     *core.PeerInfo `json:"peer"`
}

// Response defines an announce response.
type Response struct {
	Peers    []*core.PeerInfo `json:"peers"`
	Interval time.Duration    `json:"interval"`
}

// Client defines a client for announcing and getting peers.
type Client interface {
	Announce(
		name string,
		h core.InfoHash,
		complete bool,
		version int) ([]*core.PeerInfo, time.Duration, error)
}

type client struct {
	pctx  core.PeerContext
	hosts healthcheck.List
}

// New creates a new client.
func New(pctx core.PeerContext, hosts healthcheck.List) Client {
	return &client{pctx, hosts}
}

// Announce versionss.
const (
	V1 = 1
	V2 = 2
)

func getEndpoint(version int, addr string, h core.InfoHash) (method, url string) {
	if version == V1 {
		return "GET", fmt.Sprintf("http://%s/announce", addr)
	}
	return "POST", fmt.Sprintf("http://%s/announce/%s", addr, h.String())
}

// Announce announces the torrent identified by (name, h) with the number of
// downloaded bytes. Returns a list of all other peers announcing for said torrent,
// sorted by priority, and the interval for the next announce.
func (c *client) Announce(
	name string,
	h core.InfoHash,
	complete bool,
	version int) (peers []*core.PeerInfo, interval time.Duration, err error) {

	body, err := json.Marshal(&Request{
		Name:     name,
		InfoHash: h,
		Peer:     core.PeerInfoFromContext(c.pctx, complete),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %s", err)
	}
	addrs := c.hosts.Resolve().Sample(3)
	if len(addrs) == 0 {
		return nil, 0, errors.New("no hosts could be resolve")
	}
	var httpResp *http.Response
	for addr := range addrs {
		method, url := getEndpoint(version, addr, h)
		httpResp, err = httputil.Send(
			method,
			url,
			httputil.SendBody(bytes.NewReader(body)),
			httputil.SendTimeout(30*time.Second),
			httputil.SendRetry())
		if err != nil {
			if httputil.IsNetworkError(err) {
				c.hosts.Failed(addr)
				continue
			}
			return nil, 0, err
		}
		defer httpResp.Body.Close()
		var resp Response
		if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
			return nil, 0, fmt.Errorf("decode response: %s", err)
		}
		return resp.Peers, resp.Interval, nil
	}
	return nil, 0, err
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
	name string, h core.InfoHash, complete bool, version int) ([]*core.PeerInfo, time.Duration, error) {

	return nil, 0, errors.New("announcing disabled")
}
