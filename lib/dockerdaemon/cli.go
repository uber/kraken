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

	"github.com/uber/kraken/utils/log"

	"golang.org/x/net/context/ctxhttp"
)

const _defaultTimeout = 32 * time.Second

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
		if len(addr) > len(syscall.RawSockaddrUnix{}.Path) {
			return fmt.Errorf("Unix socket path %q is too long", addr)
		}

		tr.DisableCompression = true
		tr.Dial = func(_, _ string) (net.Conn, error) {
			return net.DialTimeout(protocol, addr, _defaultTimeout)
		}
		return nil
	}

	return fmt.Errorf("Protocol %s not supported", protocol)
}

// ImagePull calls `docker pull` on an image.
func (cli *dockerClient) ImagePull(ctx context.Context, registry, repo, tag string) error {
	v := url.Values{}
	fromImage := repo
	if registry != "" {
		fromImage = fmt.Sprintf("%s/%s", registry, repo)
	}
	v.Set("fromImage", fromImage)
	v.Set("tag", tag)
	headers := map[string][]string{"X-Registry-Auth": {""}}
	return cli.post(ctx, "/images/create", v, nil, headers, true)
}

func (cli *dockerClient) post(
	ctx context.Context, urlPath string, query url.Values, body io.Reader,
	header http.Header, streamRespBody bool) error {

	// Construct request. It veries depending on client version.
	var apiPath string
	if cli.version != "" {
		v := strings.TrimPrefix(cli.version, "v")
		apiPath = fmt.Sprintf("%s/v%s%s", cli.basePath, v, urlPath)
	} else {
		apiPath = fmt.Sprintf("%s%s", cli.basePath, urlPath)
	}
	u := &url.URL{Path: apiPath}
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	apiPath = u.String()
	if body == nil {
		body = bytes.NewReader([]byte{})
	}
	req, err := http.NewRequest("POST", apiPath, body)
	if err != nil {
		return fmt.Errorf("create request: %s", err)
	}
	req.Header = header
	req.Host = "docker"
	req.URL.Host = cli.addr
	req.URL.Scheme = cli.scheme

	resp, err := ctxhttp.Do(ctx, cli.client, req)
	if err != nil {
		return fmt.Errorf("send post request: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		errMsg, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read error resp: %s", err)
		}
		return fmt.Errorf("Error posting to %s: code %d, err: %s", urlPath, resp.StatusCode, errMsg)
	}

	// Docker daemon returns 200 before complete push.
	// It closes resp.Body after it finishes.
	if streamRespBody {
		log.Debugf("Streaming resp body for %s", urlPath)
		progress, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read resp body: %s", err)
		}
		log.Debugf("%s", progress)
	}

	return nil
}
