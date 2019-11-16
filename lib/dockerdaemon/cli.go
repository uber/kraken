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

	"golang.org/x/net/context/ctxhttp"
)

const _defaultTimeout = 32 * time.Second

// DockerClient is a docker daemon client.
type DockerClient interface {
	ImagePull(ctx context.Context, registry, repo, tag string) error
}

type dockerClient struct {
	version  string
	scheme   string
	addr     string
	basePath string

	client *http.Client
}

// NewDockerClient creates a new DockerClient.
func NewDockerClient(host, scheme, version string) (DockerClient, error) {
	u, err := url.Parse(host)
	if err != nil {
		return nil, fmt.Errorf("parse docker host `%s`: %s", host, err)
	}
	if u.Scheme != "unix" && u.Scheme != "http" {
		return nil, fmt.Errorf("Protocol %s not supported", u.Scheme)
	}
	if u.Scheme == "unix" && len(u.Host) > len(syscall.RawSockaddrUnix{}.Path) {
		return nil, fmt.Errorf("Unix socket path %q is too long", u.Host)
	}

	transport := new(http.Transport)
	transport.DisableCompression = true
	transport.Dial = func(_, _ string) (net.Conn, error) {
		return net.DialTimeout(u.Scheme, u.Host, _defaultTimeout)
	}
	client := &http.Client{
		Transport: transport,
	}

	return &dockerClient{
		version:  version,
		scheme:   scheme,
		addr:     u.Host,
		basePath: u.Path,
		client:   client,
	}, nil
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
	return cli.post(ctx, "/images/create", v, headers, nil, true)
}

func (cli *dockerClient) post(
	ctx context.Context, urlPath string, query url.Values, header http.Header,
	body io.Reader, streamRespBody bool) error {

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
	if body == nil {
		body = bytes.NewReader([]byte{})
	}
	req, err := http.NewRequest("POST", u.String(), body)
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

	// Docker daemon returns 200 early. Close resp.Body after reading all.
	if streamRespBody {
		if _, err := ioutil.ReadAll(resp.Body); err != nil {
			return fmt.Errorf("read resp body: %s", err)
		}
	}

	return nil
}
