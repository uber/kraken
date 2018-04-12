package announceclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
)

// Config defines Client configuration.
type Config struct {
	Timeout time.Duration `yaml:"timeout"`
}

func (c Config) applyDefaults() Config {
	if c.Timeout == 0 {
		c.Timeout = 30 * time.Second
	}
	return c
}

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
	Announce(name string, h core.InfoHash, complete bool) ([]*core.PeerInfo, time.Duration, error)
}

// HTTPClient announces to tracker over HTTP.
type HTTPClient struct {
	config  Config
	pctx    core.PeerContext
	servers serverset.Set
}

// New creates a new HTTPClient.
func New(config Config, pctx core.PeerContext, servers serverset.Set) *HTTPClient {
	config = config.applyDefaults()
	return &HTTPClient{config, pctx, servers}
}

// Default creates the default HTTPClient.
func Default(pctx core.PeerContext, servers serverset.Set) *HTTPClient {
	return New(Config{}, pctx, servers)
}

// Announce announces the torrent identified by (name, h) with the number of
// downloaded bytes. Returns a list of all other peers announcing for said torrent,
// sorted by priority, and the interval for the next announce.
func (c *HTTPClient) Announce(
	name string, h core.InfoHash, complete bool) (peers []*core.PeerInfo, interval time.Duration, err error) {

	body, err := json.Marshal(&Request{
		Name:     name,
		InfoHash: h,
		Peer:     core.PeerInfoFromContext(c.pctx, complete),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %s", err)
	}
	it := c.servers.Iter()
	for it.Next() {
		httpResp, err := httputil.Get(
			fmt.Sprintf("http://%s/announce", it.Addr()),
			httputil.SendBody(bytes.NewReader(body)),
			httputil.SendTimeout(c.config.Timeout))
		if err != nil {
			if _, ok := err.(httputil.NetworkError); ok {
				log.Errorf("Error announcing to %s: %s", it.Addr(), err)
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
	return nil, 0, it.Err()
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
	name string, h core.InfoHash, complete bool) ([]*core.PeerInfo, time.Duration, error) {

	return nil, 0, errors.New("announcing disabled")
}
