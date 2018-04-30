package tagclient

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
)

// Client errors.
var (
	ErrNotFound = errors.New("tag not found")
)

// Client wraps tagserver endpoints.
type Client interface {
	Put(tag string, d core.Digest) error
	Get(tag string) (core.Digest, error)
}

type client struct {
	servers serverset.Set
}

// New returns a new Client.
func New(servers serverset.Set) Client {
	return &client{servers}
}

func (c *client) Put(tag string, d core.Digest) error {
	it := c.servers.Iter()
	for it.Next() {
		_, err := httputil.Put(
			fmt.Sprintf("http://%s/tags/%s/digest/%s", it.Addr(), url.PathEscape(tag), d.Hex()))
		if err != nil {
			if httputil.IsNetworkError(err) {
				log.Infof("Error putting tag on %s: %s", it.Addr(), err)
				continue
			}
			return err
		}
		return nil
	}
	return it.Err()
}

func (c *client) Get(tag string) (core.Digest, error) {
	it := c.servers.Iter()
	for it.Next() {
		resp, err := httputil.Get(fmt.Sprintf("http://%s/tags/%s", it.Addr(), url.PathEscape(tag)))
		if err != nil {
			if httputil.IsNetworkError(err) {
				log.Infof("Error getting tag from %s: %s", it.Addr(), err)
				continue
			}
			if httputil.IsNotFound(err) {
				return core.Digest{}, ErrNotFound
			}
			return core.Digest{}, err
		}
		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return core.Digest{}, fmt.Errorf("read body: %s", err)
		}
		return core.NewSHA256DigestFromHex(string(b)), nil
	}
	return core.Digest{}, it.Err()
}
