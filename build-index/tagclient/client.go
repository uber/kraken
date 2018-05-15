package tagclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Client errors.
var (
	ErrNotFound = errors.New("tag not found")
)

// Provider maps addresses into Clients.
type Provider interface {
	Provide(addr string) Client
}

type provider struct{}

// NewProvider creates a new Provider.
func NewProvider() Provider { return provider{} }

func (p provider) Provide(addr string) Client { return New(addr) }

// Client wraps tagserver endpoints.
type Client interface {
	Put(tag string, d core.Digest) error
	Get(tag string) (core.Digest, error)
	Replicate(tag string, d core.Digest, dependencies []core.Digest) error
	Origin() (string, error)
}

type client struct {
	addr string
}

// New returns a new Client.
func New(addr string) Client {
	return &client{addr}
}

func (c *client) Put(tag string, d core.Digest) error {
	_, err := httputil.Put(
		fmt.Sprintf("http://%s/tags/%s/digest/%s", c.addr, url.PathEscape(tag), d.String()),
		httputil.SendRetry())
	return err
}

func (c *client) Get(tag string) (core.Digest, error) {
	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/tags/%s", c.addr, url.PathEscape(tag)),
		httputil.SendRetry())
	if err != nil {
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
	d, err := core.ParseSHA256Digest(string(b))
	if err != nil {
		return core.Digest{}, fmt.Errorf("new digest: %s", err)
	}
	return d, nil
}

// ReplicateRequest defines a Replicate request body.
type ReplicateRequest struct {
	Dependencies []string `json:"dependencies"`
}

func (c *client) Replicate(tag string, d core.Digest, dependencies []core.Digest) error {
	// Some ugliness to convert typed digests into strings.
	var strDeps []string
	for _, d := range dependencies {
		strDeps = append(strDeps, d.String())
	}
	b, err := json.Marshal(ReplicateRequest{strDeps})
	if err != nil {
		return fmt.Errorf("json marshal: %s", err)
	}
	_, err = httputil.Post(
		fmt.Sprintf("http://%s/remotes/tags/%s/digest/%s", c.addr, url.PathEscape(tag), d.String()),
		httputil.SendBody(bytes.NewReader(b)),
		httputil.SendRetry())
	return err
}

func (c *client) Origin() (string, error) {
	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/origin", c.addr),
		httputil.SendRetry())
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body: %s", err)
	}
	return string(b), nil
}
