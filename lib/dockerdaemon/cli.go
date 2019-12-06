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
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/context/ctxhttp"
)

const _defaultTimeout = 32 * time.Second

// DockerClient is a docker daemon client.
type DockerClient interface {
	PullImage(ctx context.Context, repo, tag string) error
}

type dockerClient struct {
	version  string
	scheme   string
	addr     string
	basePath string
	registry string

	client *http.Client
}

// NewDockerClient creates a new DockerClient.
func NewDockerClient(config Config, registry string) (DockerClient, error) {
	config = config.applyDefaults()

	client, addr, basePath, err := parseHost(config.DockerHost)
	if err != nil {
		return nil, fmt.Errorf("parse docker host %q: %s", config.DockerHost, err)
	}

	return &dockerClient{
		version:  config.DockerClientVersion,
		scheme:   config.DockerScheme,
		addr:     addr,
		basePath: basePath,
		registry: registry,
		client:   client,
	}, nil
}

// parseHost parses host URL and returns a HTTP client.
// This is needed because url.Parse cannot correctly parse url of format
// "unix:///...".
func parseHost(host string) (*http.Client, string, string, error) {
	strs := strings.SplitN(host, "://", 2)
	if len(strs) == 1 {
		return nil, "", "", fmt.Errorf("unable to parse docker host `%s`", host)
	}

	var basePath string
	transport := new(http.Transport)

	protocol, addr := strs[0], strs[1]
	if protocol == "tcp" {
		parsed, err := url.Parse("tcp://" + addr)
		if err != nil {
			return nil, "", "", err
		}
		addr = parsed.Host
		basePath = parsed.Path
	} else if protocol == "unix" {
		if len(addr) > len(syscall.RawSockaddrUnix{}.Path) {
			return nil, "", "", fmt.Errorf("unix socket path %q is too long", addr)
		}
		transport.DisableCompression = true
		transport.Dial = func(_, _ string) (net.Conn, error) {
			return net.DialTimeout(protocol, addr, _defaultTimeout)
		}
	} else {
		return nil, "", "", fmt.Errorf("protocol %s not supported", protocol)
	}

	client := &http.Client{
		Transport: transport,
	}
	return client, addr, basePath, nil
}

// ImagePull calls `docker pull` on an image from known registry.
func (cli *dockerClient) PullImage(ctx context.Context, repo, tag string) error {
	query := url.Values{}
	fromImage := fmt.Sprintf("%s/%s", cli.registry, repo)
	query.Set("fromImage", fromImage)
	query.Set("tag", tag)
	headers := map[string][]string{"X-Registry-Auth": {""}}
	urlPath := "/images/create"

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
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader([]byte{}))
	if err != nil {
		return fmt.Errorf("create request: %s", err)
	}
	req.Header = headers
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

	// Docker daemon returns 200 early. Close resp.Body after reading all.
	if _, err := ioutil.ReadAll(resp.Body); err != nil {
		return fmt.Errorf("read resp body: %s", err)
	}

	return nil
}
