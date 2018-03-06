package tagclient

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
)

// Client errors.
var (
	ErrNotFound = errors.New("tag not found")
)

const _timeout = 15 * time.Second

// Client performs tag to value lookups.
type Client interface {
	Get(name string) (string, error)
}

type client struct {
	servers serverset.Set
}

// New creates a new Client.
func New(servers serverset.Set) Client {
	return &client{servers}
}

func (c *client) Get(name string) (string, error) {
	it := c.servers.Iter()
	for it.Next() {
		resp, err := httputil.Get(
			fmt.Sprintf("http://%s/tag/%s", it.Addr(), url.PathEscape(name)),
			httputil.SendTimeout(_timeout))
		if err != nil {
			if _, ok := err.(httputil.NetworkError); ok {
				log.Errorf("Error geting tag from %s: %s", it.Addr(), err)
				continue
			}
			if httputil.IsNotFound(err) {
				err = ErrNotFound
			}
			return "", err
		}
		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("read body: %s", err)
		}
		return string(b), nil
	}
	return "", it.Err()
}
