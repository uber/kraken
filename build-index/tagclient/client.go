package tagclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/healthcheck"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Client errors.
var (
	ErrTagNotFound = errors.New("tag not found")
)

// Client wraps tagserver endpoints.
type Client interface {
	Put(tag string, d core.Digest) error
	PutAndReplicate(tag string, d core.Digest) error
	Get(tag string) (core.Digest, error)
	Has(tag string) (bool, error)
	List(prefix string) ([]string, error)
	ListRepository(repo string) ([]string, error)
	Replicate(tag string) error
	Origin() (string, error)

	DuplicateReplicate(
		tag string, d core.Digest, dependencies core.DigestList, delay time.Duration) error
	DuplicatePut(tag string, d core.Digest, delay time.Duration) error
}

type singleClient struct {
	addr string
}

// NewSingleClient returns a Client scoped to a single tagserver instance.
func NewSingleClient(addr string) Client {
	return &singleClient{addr}
}

func (c *singleClient) Put(tag string, d core.Digest) error {
	_, err := httputil.Put(
		fmt.Sprintf("http://%s/tags/%s/digest/%s", c.addr, url.PathEscape(tag), d.String()),
		httputil.SendTimeout(30*time.Second))
	return err
}

func (c *singleClient) PutAndReplicate(tag string, d core.Digest) error {
	_, err := httputil.Put(
		fmt.Sprintf("http://%s/tags/%s/digest/%s?replicate=true", c.addr, url.PathEscape(tag), d.String()),
		httputil.SendTimeout(30*time.Second))
	return err
}

func (c *singleClient) Get(tag string) (core.Digest, error) {
	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/tags/%s", c.addr, url.PathEscape(tag)),
		httputil.SendTimeout(10*time.Second))
	if err != nil {
		if httputil.IsNotFound(err) {
			return core.Digest{}, ErrTagNotFound
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

func (c *singleClient) Has(tag string) (bool, error) {
	_, err := httputil.Head(
		fmt.Sprintf("http://%s/tags/%s", c.addr, url.PathEscape(tag)),
		httputil.SendTimeout(10*time.Second))
	if err != nil {
		if httputil.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *singleClient) List(prefix string) ([]string, error) {
	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/list/%s", c.addr, prefix),
		httputil.SendTimeout(60*time.Second))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var names []string
	if err := json.NewDecoder(resp.Body).Decode(&names); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return names, nil
}

// XXX: Deprecated. Use List instead.
func (c *singleClient) ListRepository(repo string) ([]string, error) {
	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/repositories/%s/tags", c.addr, url.PathEscape(repo)),
		httputil.SendTimeout(60*time.Second))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var tags []string
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return tags, nil
}

// ReplicateRequest defines a Replicate request body.
type ReplicateRequest struct {
	Dependencies []core.Digest `json:"dependencies"`
}

func (c *singleClient) Replicate(tag string) error {
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/remotes/tags/%s", c.addr, url.PathEscape(tag)),
		httputil.SendTimeout(15*time.Second))
	return err
}

// DuplicateReplicateRequest defines a DuplicateReplicate request body.
type DuplicateReplicateRequest struct {
	Dependencies core.DigestList `json:"dependencies"`
	Delay        time.Duration   `json:"delay"`
}

func (c *singleClient) DuplicateReplicate(
	tag string, d core.Digest, dependencies core.DigestList, delay time.Duration) error {

	b, err := json.Marshal(DuplicateReplicateRequest{dependencies, delay})
	if err != nil {
		return fmt.Errorf("json marshal: %s", err)
	}
	_, err = httputil.Post(
		fmt.Sprintf(
			"http://%s/internal/duplicate/remotes/tags/%s/digest/%s",
			c.addr, url.PathEscape(tag), d.String()),
		httputil.SendBody(bytes.NewReader(b)),
		httputil.SendTimeout(10*time.Second),
		httputil.SendRetry())
	return err
}

// DuplicatePutRequest defines a DuplicatePut request body.
type DuplicatePutRequest struct {
	Delay time.Duration `json:"delay"`
}

func (c *singleClient) DuplicatePut(tag string, d core.Digest, delay time.Duration) error {
	b, err := json.Marshal(DuplicatePutRequest{delay})
	if err != nil {
		return fmt.Errorf("json marshal: %s", err)
	}
	_, err = httputil.Put(
		fmt.Sprintf(
			"http://%s/internal/duplicate/tags/%s/digest/%s",
			c.addr, url.PathEscape(tag), d.String()),
		httputil.SendBody(bytes.NewReader(b)),
		httputil.SendTimeout(10*time.Second),
		httputil.SendRetry())
	return err
}

func (c *singleClient) Origin() (string, error) {
	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/origin", c.addr),
		httputil.SendTimeout(5*time.Second))
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

type clusterClient struct {
	hosts healthcheck.List
}

// NewClusterClient creates a Client which operates on tagserver instances as
// a cluster.
func NewClusterClient(hosts healthcheck.List) Client {
	return &clusterClient{hosts}
}

func (cc *clusterClient) do(request func(c Client) error) error {
	addrs := cc.hosts.Resolve().Sample(3)
	if len(addrs) == 0 {
		return errors.New("cluster client: no hosts could be resolved")
	}
	var err error
	for addr := range addrs {
		err = request(NewSingleClient(addr))
		if httputil.IsNetworkError(err) {
			cc.hosts.Failed(addr)
			continue
		}
		break
	}
	return err
}

func (cc *clusterClient) Put(tag string, d core.Digest) error {
	return cc.do(func(c Client) error { return c.Put(tag, d) })
}

func (cc *clusterClient) PutAndReplicate(tag string, d core.Digest) error {
	return cc.do(func(c Client) error { return c.PutAndReplicate(tag, d) })
}

func (cc *clusterClient) Get(tag string) (d core.Digest, err error) {
	err = cc.do(func(c Client) error {
		d, err = c.Get(tag)
		return err
	})
	return
}

func (cc *clusterClient) Has(tag string) (ok bool, err error) {
	err = cc.do(func(c Client) error {
		ok, err = c.Has(tag)
		return err
	})
	return
}

func (cc *clusterClient) List(prefix string) (tags []string, err error) {
	err = cc.do(func(c Client) error {
		tags, err = c.List(prefix)
		return err
	})
	return
}

func (cc *clusterClient) ListRepository(repo string) (tags []string, err error) {
	err = cc.do(func(c Client) error {
		tags, err = c.ListRepository(repo)
		return err
	})
	return
}

func (cc *clusterClient) Replicate(tag string) error {
	return cc.do(func(c Client) error { return c.Replicate(tag) })
}

func (cc *clusterClient) Origin() (origin string, err error) {
	err = cc.do(func(c Client) error {
		origin, err = c.Origin()
		return err
	})
	return
}

func (cc *clusterClient) DuplicateReplicate(
	tag string, d core.Digest, dependencies core.DigestList, delay time.Duration) error {

	return errors.New("duplicate replicate not supported on cluster client")
}

func (cc *clusterClient) DuplicatePut(tag string, d core.Digest, delay time.Duration) error {
	return errors.New("duplicate put not supported on cluster client")
}
