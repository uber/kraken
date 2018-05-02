package tagclient

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	"code.uber.internal/infra/kraken/utils/httputil"
)

// Client errors.
var (
	ErrNotFound = errors.New("tag not found")
)

// Client performs tag to value lookups.
type Client interface {
	Get(name string) (string, error)
}

type client struct {
	addr string
}

// New creates a new Client.
func New(addr string) Client {
	return &client{addr}
}

func (c *client) Get(name string) (string, error) {
	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/tag/%s", c.addr, url.PathEscape(name)),
		httputil.SendTimeout(15*time.Second),
		httputil.SendRetry())
	if err != nil {
		if httputil.IsNotFound(err) {
			return "", ErrNotFound
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
