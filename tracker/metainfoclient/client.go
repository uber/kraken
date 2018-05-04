package metainfoclient

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/backoff"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Client errors.
var (
	ErrNotFound = errors.New("metainfo not found")
)

// Client defines operations on torrent metainfo.
type Client interface {
	Download(namespace, name string) (*core.MetaInfo, error)
}

type client struct {
	addr    string
	backoff *backoff.Backoff
}

// New returns a new Client.
func New(addr string) Client {
	return NewWithBackoff(addr, backoff.New(backoff.Config{RetryTimeout: 15 * time.Minute}))
}

// NewWithBackoff returns a new Client with custom backoff.
func NewWithBackoff(addr string, b *backoff.Backoff) Client {
	return &client{addr, b}
}

// Download returns the MetaInfo associated with name. Returns ErrNotFound if
// no torrent exists under name.
func (c *client) Download(namespace, name string) (*core.MetaInfo, error) {
	d := core.NewSHA256DigestFromHex(name)
	resp, err := httputil.PollAccepted(
		fmt.Sprintf(
			"http://%s/namespace/%s/blobs/%s/metainfo",
			c.addr, url.PathEscape(namespace), d),
		c.backoff,
		httputil.SendTimeout(30*time.Second),
		httputil.SendRetry())
	if err != nil {
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
