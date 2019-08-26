// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package tagclient

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"strconv"
	"time"

	"github.com/uber/kraken/build-index/tagmodels"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/utils/httputil"
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
	ListWithPagination(prefix string, filter ListFilter) (tagmodels.ListResponse, error)
	ListRepository(repo string) ([]string, error)
	ListRepositoryWithPagination(repo string, filter ListFilter) (tagmodels.ListResponse, error)
	Replicate(tag string) error
	Origin() (string, error)

	DuplicateReplicate(
		tag string, d core.Digest, dependencies core.DigestList, delay time.Duration) error
	DuplicatePut(tag string, d core.Digest, delay time.Duration) error
}

type singleClient struct {
	addr string
	tls  *tls.Config
}

// ListFilter contains filter request for list with pagination operations.
type ListFilter struct {
	Offset string
	Limit  int
}

// NewSingleClient returns a Client scoped to a single tagserver instance.
func NewSingleClient(addr string, config *tls.Config) Client {
	return &singleClient{addr, config}
}

func (c *singleClient) Put(tag string, d core.Digest) error {
	_, err := httputil.Put(
		fmt.Sprintf("http://%s/tags/%s/digest/%s", c.addr, url.PathEscape(tag), d.String()),
		httputil.SendTimeout(30*time.Second),
		httputil.SendTLS(c.tls))
	return err
}

func (c *singleClient) PutAndReplicate(tag string, d core.Digest) error {
	_, err := httputil.Put(
		fmt.Sprintf("http://%s/tags/%s/digest/%s?replicate=true", c.addr, url.PathEscape(tag), d.String()),
		httputil.SendTimeout(30*time.Second),
		httputil.SendTLS(c.tls))
	return err
}

func (c *singleClient) Get(tag string) (core.Digest, error) {
	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/tags/%s", c.addr, url.PathEscape(tag)),
		httputil.SendTimeout(10*time.Second),
		httputil.SendTLS(c.tls))
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
		httputil.SendTimeout(10*time.Second),
		httputil.SendTLS(c.tls))
	if err != nil {
		if httputil.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *singleClient) doListPaginated(urlFormat string, pathSub string,
	filter ListFilter) (tagmodels.ListResponse, error) {

	// Build query.
	reqVal := url.Values{}
	if filter.Offset != "" {
		reqVal.Add(tagmodels.OffsetQ, filter.Offset)
	}
	if filter.Limit != 0 {
		reqVal.Add(tagmodels.LimitQ, strconv.Itoa(filter.Limit))
	}

	// Fetch list response from server.
	serverUrl := url.URL{
		Scheme:   "http",
		Host:     c.addr,
		Path:     fmt.Sprintf(urlFormat, pathSub),
		RawQuery: reqVal.Encode(),
	}
	var resp tagmodels.ListResponse
	httpResp, err := httputil.Get(
		serverUrl.String(),
		httputil.SendTimeout(60*time.Second),
		httputil.SendTLS(c.tls))
	if err != nil {
		return resp, err
	}
	defer httpResp.Body.Close()
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return resp, fmt.Errorf("json decode: %s", err)
	}

	return resp, nil
}

func (c *singleClient) doList(pathSub string,
	fn func(pathSub string, filter ListFilter) (tagmodels.ListResponse, error)) (
	[]string, error) {

	var names []string

	offset := ""
	for ok := true; ok; ok = (offset != "") {
		filter := ListFilter{Offset: offset}
		resp, err := fn(pathSub, filter)
		if err != nil {
			return nil, err
		}
		offset, err = resp.GetOffset()
		if err != nil && err != io.EOF {
			return nil, err
		}
		names = append(names, resp.Result...)
	}
	return names, nil
}

func (c *singleClient) List(prefix string) ([]string, error) {
	return c.doList(prefix, func(prefix string, filter ListFilter) (
		tagmodels.ListResponse, error) {

		return c.ListWithPagination(prefix, filter)
	})
}

func (c *singleClient) ListWithPagination(prefix string, filter ListFilter) (
	tagmodels.ListResponse, error) {

	return c.doListPaginated("list/%s", prefix, filter)
}

// XXX: Deprecated. Use List instead.
func (c *singleClient) ListRepository(repo string) ([]string, error) {
	return c.doList(repo, func(repo string, filter ListFilter) (
		tagmodels.ListResponse, error) {

		return c.ListRepositoryWithPagination(repo, filter)
	})
}

func (c *singleClient) ListRepositoryWithPagination(repo string,
	filter ListFilter) (tagmodels.ListResponse, error) {

	return c.doListPaginated("repositories/%s/tags", url.PathEscape(repo), filter)
}

// ReplicateRequest defines a Replicate request body.
type ReplicateRequest struct {
	Dependencies []core.Digest `json:"dependencies"`
}

func (c *singleClient) Replicate(tag string) error {
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/remotes/tags/%s", c.addr, url.PathEscape(tag)),
		httputil.SendTimeout(15*time.Second),
		httputil.SendTLS(c.tls))
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
		httputil.SendRetry(),
		httputil.SendTLS(c.tls))
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
		httputil.SendRetry(),
		httputil.SendTLS(c.tls))
	return err
}

func (c *singleClient) Origin() (string, error) {
	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/origin", c.addr),
		httputil.SendTimeout(5*time.Second),
		httputil.SendTLS(c.tls))
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
	tls   *tls.Config
}

// NewClusterClient creates a Client which operates on tagserver instances as
// a cluster.
func NewClusterClient(hosts healthcheck.List, config *tls.Config) Client {
	return &clusterClient{hosts, config}
}

func (cc *clusterClient) do(request func(c Client) error) error {
	addrs := cc.hosts.Resolve().Sample(3)
	if len(addrs) == 0 {
		return errors.New("cluster client: no hosts could be resolved")
	}
	var err error
	for addr := range addrs {
		err = request(NewSingleClient(addr, cc.tls))
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

func (cc *clusterClient) ListWithPagination(prefix string, filter ListFilter) (
	resp tagmodels.ListResponse, err error) {

	err = cc.do(func(c Client) error {
		resp, err = c.ListWithPagination(prefix, filter)
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

func (cc *clusterClient) ListRepositoryWithPagination(repo string,
	filter ListFilter) (resp tagmodels.ListResponse, err error) {

	err = cc.do(func(c Client) error {
		resp, err = c.ListRepositoryWithPagination(repo, filter)
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
