package metainfoclient

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/backoff"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
)

// Client errors.
var (
	ErrNotFound = errors.New("metainfo not found")
)

// Config defines Client configuration.
type Config struct {
	Timeout     time.Duration  `yaml:"timeout"`
	PollBackoff backoff.Config `yaml:"poll_backoff"`
}

func (c Config) applyDefaults() Config {
	if c.Timeout == 0 {
		c.Timeout = 30 * time.Second
	}
	if c.PollBackoff.RetryTimeout == 0 {
		c.PollBackoff.RetryTimeout = 15 * time.Minute
	}
	return c
}

// Client defines operations on torrent metainfo.
type Client interface {
	Download(namespace string, name string) (*torlib.MetaInfo, error)
}

// Getter performs HTTP get requests on some url.
type Getter interface {
	Get(url string) (*http.Response, error)
}

type client struct {
	config      Config
	servers     serverset.Set
	getter      Getter
	pollBackoff *backoff.Backoff
}

type clientOpts struct {
	getter Getter
}

// Option defines a Client option.
type Option func(*clientOpts)

// WithGetter overrides the default Client Getter with g.
func WithGetter(g Getter) Option {
	return func(o *clientOpts) { o.getter = g }
}

// New returns a new Client.
func New(config Config, servers serverset.Set, opts ...Option) Client {
	config = config.applyDefaults()

	defaults := &clientOpts{
		getter: &http.Client{Timeout: config.Timeout},
	}
	for _, opt := range opts {
		opt(defaults)
	}

	return &client{config, servers, defaults.getter, backoff.New(config.PollBackoff)}
}

// Default returns a default Client.
func Default(servers serverset.Set) Client {
	return New(Config{}, servers)
}

// Download returns the MetaInfo associated with name. Returns ErrNotFound if
// no torrent exists under name.
func (c *client) Download(namespace string, name string) (*torlib.MetaInfo, error) {
	d := image.NewSHA256DigestFromHex(name)
	it := c.servers.Iter()
SERVERS:
	for it.Next() {
		a := c.pollBackoff.Attempts()
	POLL:
		for a.WaitForNext() {
			resp, err := c.getter.Get(
				fmt.Sprintf("http://%s/namespace/%s/blobs/%s/metainfo", it.Addr(), namespace, d))
			if err != nil {
				log.Errorf("Error downloading metainfo from %s: %s", it.Addr(), err)
				continue SERVERS
			}
			switch resp.StatusCode {
			case http.StatusOK:
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return nil, fmt.Errorf("read body: %s", err)
				}
				mi, err := torlib.DeserializeMetaInfo(b)
				if err != nil {
					return nil, fmt.Errorf("deserialize metainfo: %s", err)
				}
				return mi, nil
			case http.StatusAccepted:
				continue POLL
			case http.StatusNotFound:
				return nil, ErrNotFound
			default:
				return nil, httputil.NewStatusError(resp)
			}
		}
		return nil, a.Err()
	}
	return nil, it.Err()
}
