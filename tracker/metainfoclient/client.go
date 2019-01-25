package metainfoclient

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/utils/backoff"
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
	ring    hashring.PassiveRing
	tls     *tls.Config
	backoff *backoff.Backoff
}

// New returns a new Client.
func New(ring hashring.PassiveRing, tls *tls.Config) Client {
	return &client{ring, tls, backoff.New(backoff.Config{RetryTimeout: 15 * time.Minute})}
}

// Download returns the MetaInfo associated with name. Returns ErrNotFound if
// no torrent exists under name.
func (c *client) Download(namespace string, d core.Digest) (*core.MetaInfo, error) {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     time.Second,
		RandomizationFactor: 0.05,
		Multiplier:          1.3,
		MaxInterval:         5 * time.Second,
		MaxElapsedTime:      15 * time.Minute,
	}
	var resp *http.Response
	var err error
	for _, addr := range c.ring.Locations(d) {
		resp, err = httputil.PollAccepted(
			fmt.Sprintf(
				"http://%s/namespace/%s/blobs/%s/metainfo",
				addr, url.PathEscape(namespace), d),
			c.backoff,
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
