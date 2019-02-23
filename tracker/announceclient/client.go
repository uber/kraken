// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package announceclient

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/utils/httputil"
)

// ErrDisabled is returned when announce is disabled.
var ErrDisabled = errors.New("announcing disabled")

// Request defines an announce request.
type Request struct {
	Name     string         `json:"name"`
	Digest   *core.Digest   `json:"digest"` // Optional (for now).
	InfoHash core.InfoHash  `json:"info_hash"`
	Peer     *core.PeerInfo `json:"peer"`
}

// GetDigest is a backwards compatible accessor of the request digest.
func (r *Request) GetDigest() (core.Digest, error) {
	if r.Digest != nil {
		return *r.Digest, nil
	}
	d, err := core.NewSHA256DigestFromHex(r.Name)
	if err != nil {
		return core.Digest{}, err
	}
	return d, nil
}

// Response defines an announce response.
type Response struct {
	Peers    []*core.PeerInfo `json:"peers"`
	Interval time.Duration    `json:"interval"`
}

// Client defines a client for announcing and getting peers.
type Client interface {
	Announce(
		d core.Digest,
		h core.InfoHash,
		complete bool,
		version int) ([]*core.PeerInfo, time.Duration, error)
}

type client struct {
	pctx core.PeerContext
	ring hashring.PassiveRing
	tls  *tls.Config
}

// New creates a new client.
func New(pctx core.PeerContext, ring hashring.PassiveRing, tls *tls.Config) Client {
	return &client{pctx, ring, tls}
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

// Announce announces the torrent identified by (d, h) with the number of
// downloaded bytes. Returns a list of all other peers announcing for said torrent,
// sorted by priority, and the interval for the next announce.
func (c *client) Announce(
	d core.Digest,
	h core.InfoHash,
	complete bool,
	version int) (peers []*core.PeerInfo, interval time.Duration, err error) {

	body, err := json.Marshal(&Request{
		Name:     d.Hex(), // For backwards compatability. TODO(codyg): Remove.
		Digest:   &d,
		InfoHash: h,
		Peer:     core.PeerInfoFromContext(c.pctx, complete),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %s", err)
	}
	var httpResp *http.Response
	for _, addr := range c.ring.Locations(d) {
		method, url := getEndpoint(version, addr, h)
		httpResp, err = httputil.Send(
			method,
			url,
			httputil.SendBody(bytes.NewReader(body)),
			httputil.SendTimeout(10*time.Second),
			httputil.SendTLS(c.tls))
		if err != nil {
			if httputil.IsNetworkError(err) {
				c.ring.Failed(addr)
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
	d core.Digest, h core.InfoHash, complete bool, version int) ([]*core.PeerInfo, time.Duration, error) {

	return nil, 0, ErrDisabled
}
