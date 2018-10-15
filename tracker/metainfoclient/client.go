package metainfoclient

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/healthcheck"
	"code.uber.internal/infra/kraken/utils/backoff"
	"code.uber.internal/infra/kraken/utils/httputil"
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
	hosts   healthcheck.List
	backoff *backoff.Backoff
}

// New returns a new Client.
func New(hosts healthcheck.List) Client {
	return &client{hosts, backoff.New(backoff.Config{RetryTimeout: 15 * time.Minute})}
}

// Download returns the MetaInfo associated with name. Returns ErrNotFound if
// no torrent exists under name.
func (c *client) Download(namespace string, d core.Digest) (*core.MetaInfo, error) {
	addrs := c.hosts.Resolve().Sample(3)
	if len(addrs) == 0 {
		return nil, errors.New("no hosts could be resolved")
	}
	var resp *http.Response
	var err error
	for addr := range addrs {
		resp, err = httputil.PollAccepted(
			fmt.Sprintf(
				"http://%s/namespace/%s/blobs/%s/metainfo",
				addr, url.PathEscape(namespace), d),
			c.backoff,
			httputil.SendTimeout(15*time.Second))
		if err != nil {
			if httputil.IsNetworkError(err) {
				c.hosts.Failed(addr)
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
