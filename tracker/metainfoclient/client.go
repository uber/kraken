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
package metainfoclient

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/utils/httputil"
)

// Client errors.
var (
	ErrNotFound = errors.New("metainfo not found")
)

// Client defines operations on torrent metainfo.
type Client interface {
	Download(namespace string, d core.Digest) (*core.MetaInfo, error)
}

type client struct {
	ring hashring.PassiveRing
	tls  *tls.Config
}

// New returns a new Client.
func New(ring hashring.PassiveRing, tls *tls.Config) Client {
	return &client{ring, tls}
}

// Download returns the MetaInfo associated with name. Returns ErrNotFound if
// no torrent exists under name.
func (c *client) Download(namespace string, d core.Digest) (*core.MetaInfo, error) {
	var resp *http.Response
	var err error
	for _, addr := range c.ring.Locations(d) {
		resp, err = httputil.PollAccepted(
			fmt.Sprintf(
				"http://%s/namespace/%s/blobs/%s/metainfo",
				addr, url.PathEscape(namespace), d),
			&backoff.ExponentialBackOff{
				InitialInterval:     time.Second,
				RandomizationFactor: 0.05,
				Multiplier:          1.3,
				MaxInterval:         5 * time.Second,
				MaxElapsedTime:      15 * time.Minute,
				Clock:               backoff.SystemClock,
			},
			httputil.SendTimeout(10*time.Second),
			httputil.SendTLS(c.tls))
		if err != nil {
			if httputil.IsNetworkError(err) {
				c.ring.Failed(addr)
				continue
			}
			if httputil.IsNotFound(err) {
				return nil, ErrNotFound
			}
			return nil, err
		}
		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read body: %s", err)
		}
		mi, err := core.DeserializeMetaInfo(b)
		if err != nil {
			return nil, fmt.Errorf("deserialize metainfo: %s", err)
		}
		return mi, nil
	}
	return nil, err
}
