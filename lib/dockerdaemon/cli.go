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
package dockerdaemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/uber/makisu/lib/log"

	"golang.org/x/net/context/ctxhttp"
)

const maxUnixSocketPathSize = len(syscall.RawSockaddrUnix{}.Path)
const defaultTimeout = 32 * time.Second

// DockerClient is a docker daemon client.
type DockerClient interface {
	ImagePull(ctx context.Context, registry, repo, tag string) error
}

type dockerClient struct {
	version string // docker version
	host    string // host that client connects to
	scheme  string // http/https

	addr     string       // client address
	protocol string       // unix
	basePath string       // base part of the url
	client   *http.Client // opens http.transport
}

// NewDockerClient creates a new DockerClient.
func NewDockerClient(host, scheme, version string) (DockerClient, error) {
	protocol, addr, basePath, err := parseHost(host)
	if err != nil {
		return nil, err
	}

	transport := new(http.Transport)
	configureTransport(transport, protocol, addr)
	client := &http.Client{
		Transport: transport,
	}

	return &dockerClient{
		scheme:   scheme,
		host:     host,
		version:  version,
		protocol: protocol,
		addr:     addr,
		basePath: basePath,
		client:   client,
	}, nil
}

func parseHost(host string) (string, string, string, error) {
	strs := strings.SplitN(host, "://", 2)
	if len(strs) == 1 {
		return "", "", "", fmt.Errorf("unable to parse docker host `%s`", host)
	}

	var basePath string
	protocol, addr := strs[0], strs[1]
	if protocol == "tcp" {
		parsed, err := url.Parse("tcp://" + addr)
		if err != nil {
			return "", "", "", err
		}
		addr = parsed.Host
		basePath = parsed.Path
	}
	return protocol, addr, basePath, nil
}

func configureTransport(tr *http.Transport, protocol, addr string) error {
	switch protocol {
	case "unix":
		if len(addr) > maxUnixSocketPathSize {
			return fmt.Errorf("Unix socket path %q is too long", addr)
		}

		tr.DisableCompression = true
		tr.Dial = func(_, _ string) (net.Conn, error) {
			return net.DialTimeout(protocol, addr, defaultTimeout)
		}
		return nil
	}

	return fmt.Errorf("Protocol %s not supported", protocol)
}

func (cli *dockerClient) post(
	ctx context.Context, url string, query url.Values, body io.Reader,
	header http.Header, streamRespBody bool) error {

	if body == nil {
		body = bytes.NewReader([]byte{})
	}
	resp, err := cli.doRequest(ctx, "POST", cli.getAPIPath(url, query), body, header)
	if err != nil {
		return fmt.Errorf("post request: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		errMsg, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read error resp: %s", err)
		}
		return fmt.Errorf("Error posting to %s: code %d, err: %s", url, resp.StatusCode, errMsg)
	}

	// Docker daemon returns 200 before complete push
	// it closes resp.Body after it finishes
	if streamRespBody {
		log.Debugf("Streaming resp body for %s", url)
		progress, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read resp body: %s", err)
		}
		log.Debugf("%s", progress)
	}

	return nil
}

func (cli *dockerClient) getAPIPath(p string, query url.Values) string {
	var apiPath string
	if cli.version != "" {
		v := strings.TrimPrefix(cli.version, "v")
		apiPath = fmt.Sprintf("%s/v%s%s", cli.basePath, v, p)
	} else {
		apiPath = fmt.Sprintf("%s%s", cli.basePath, p)
	}

	u := &url.URL{
		Path: apiPath,
	}
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	return u.String()
}

func (cli *dockerClient) doRequest(
	ctx context.Context,
	method string,
	url string,
	body io.Reader,
	header http.Header) (*http.Response, error) {

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header = header
	req.Host = "docker"
	req.URL.Host = cli.addr
	req.URL.Scheme = cli.scheme

	return ctxhttp.Do(ctx, cli.client, req)
}
