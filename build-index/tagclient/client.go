package tagclient

import (
	"bytes"
	"encoding/json"
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

// Provider maps locations to Clients.
type Provider interface {
	Provide(loc string) Client
}

// DNSProvider creates Clients from DNS records.
type DNSProvider struct{}

// NewDNSProvider returns a new DNSProvider.
func NewDNSProvider() DNSProvider { return DNSProvider{} }

// Provide returns a Client for the given dns record.
func (p DNSProvider) Provide(dns string) Client {
	return New(serverset.DNSRoundRobin(dns))
}

// Client wraps tagserver endpoints.
type Client interface {
	Put(tag string, d core.Digest) error
	Get(tag string) (core.Digest, error)
	Replicate(tag string, d core.Digest, dependencies []core.Digest) error
	Origin() (string, error)
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

// ReplicateRequest defines a Replicate request body.
type ReplicateRequest struct {
	Dependencies []string `json:"dependencies"`
}

func (c *client) Replicate(tag string, d core.Digest, dependencies []core.Digest) error {
	it := c.servers.Iter()
	for it.Next() {
		// Some ugliness to convert typed digests into strings.
		var strDeps []string
		for _, d := range dependencies {
			strDeps = append(strDeps, d.Hex())
		}
		b, err := json.Marshal(ReplicateRequest{strDeps})
		if err != nil {
			return fmt.Errorf("json marshal: %s", err)
		}
		_, err = httputil.Post(
			fmt.Sprintf("http://%s/remotes/tags/%s/digest/%s", it.Addr(), url.PathEscape(tag), d.Hex()),
			httputil.SendBody(bytes.NewReader(b)))
		if err != nil {
			if httputil.IsNetworkError(err) {
				log.Infof("Error replicating tag to %s: %s", it.Addr(), err)
				continue
			}
			return err
		}
		return nil
	}
	return it.Err()
}

func (c *client) Origin() (string, error) {
	it := c.servers.Iter()
	for it.Next() {
		resp, err := httputil.Get(fmt.Sprintf("http://%s/origin", it.Addr()))
		if err != nil {
			if httputil.IsNetworkError(err) {
				log.Infof("Error getting origin from %s: %s", it.Addr(), err)
				continue
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
