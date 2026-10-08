// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dockerdaemon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseHost(t *testing.T) {
	require := require.New(t)

	// Valid TCP host
	client, addr, basePath, err := parseHost("tcp://127.0.0.1:2375")
	require.NoError(err)
	require.NotNil(client)
	require.Equal("127.0.0.1:2375", addr)
	require.Equal("", basePath)

	// Valid TCP host with path
	client, addr, basePath, err = parseHost("tcp://127.0.0.1:2375/prefix")
	require.NoError(err)
	require.NotNil(client)
	require.Equal("127.0.0.1:2375", addr)
	require.Equal("/prefix", basePath)

	// Valid unix host
	client, addr, basePath, err = parseHost("unix:///var/run/docker.sock")
	require.NoError(err)
	require.NotNil(client)
	require.Equal("/var/run/docker.sock", addr)
	require.Equal("", basePath)

	// Invalid format: missing scheme separator
	_, _, _, err = parseHost("invalid-host")
	require.Error(err)
	require.Contains(err.Error(), "unable to parse docker host")

	// Unsupported protocol
	_, _, _, err = parseHost("udp://127.0.0.1:2375")
	require.Error(err)
	require.Contains(err.Error(), "protocol udp not supported")

	// Unix socket path too long
	longPath := "unix:///" + strings.Repeat("a", 200)
	_, _, _, err = parseHost(longPath)
	require.Error(err)
	require.Contains(err.Error(), "unix socket path")
	require.Contains(err.Error(), "is too long")
}

func TestNewDockerClient(t *testing.T) {
	require := require.New(t)

	config := Config{
		DockerHost:          "tcp://127.0.0.1:2375",
		DockerScheme:        "http",
		DockerClientVersion: "1.24",
	}
	cli, err := NewDockerClient(config, "example.com:5000")
	require.NoError(err)
	require.NotNil(cli)

	dcli, ok := cli.(*dockerClient)
	require.True(ok)
	require.Equal("1.24", dcli.version)
	require.Equal("http", dcli.scheme)
	require.Equal("127.0.0.1:2375", dcli.addr)
	require.Equal("example.com:5000", dcli.registry)
}

func TestPullImageSuccess(t *testing.T) {
	require := require.New(t)

	var receivedMethod, receivedPath, receivedAuth, receivedFromImage, receivedTag string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		receivedPath = r.URL.Path
		receivedAuth = r.Header.Get("X-Registry-Auth")
		receivedFromImage = r.URL.Query().Get("fromImage")
		receivedTag = r.URL.Query().Get("tag")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"downloading"}`))
	}))
	defer server.Close()

	// Extract server host and scheme
	serverURL := strings.TrimPrefix(server.URL, "http://")

	cli := &dockerClient{
		version:  "1.24",
		scheme:   "http",
		addr:     serverURL,
		basePath: "",
		registry: "registry.example.com",
		client:   server.Client(),
	}

	err := cli.PullImage(context.Background(), "repo/app", "v1.0.0")
	require.NoError(err)

	require.Equal("POST", receivedMethod)
	require.Equal("/v1.24/images/create", receivedPath)
	require.Equal("", receivedAuth)
	require.Equal("registry.example.com/repo/app", receivedFromImage)
	require.Equal("v1.0.0", receivedTag)
}

func TestPullImageErrorResponse(t *testing.T) {
	require := require.New(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("image not found"))
	}))
	defer server.Close()

	serverURL := strings.TrimPrefix(server.URL, "http://")

	cli := &dockerClient{
		version:  "",
		scheme:   "http",
		addr:     serverURL,
		basePath: "",
		registry: "registry.example.com",
		client:   server.Client(),
	}

	err := cli.PullImage(context.Background(), "nonexistent", "latest")
	require.Error(err)
	require.Contains(err.Error(), "code 404")
	require.Contains(err.Error(), "image not found")
}

func TestPullImageContextCanceled(t *testing.T) {
	require := require.New(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	serverURL := strings.TrimPrefix(server.URL, "http://")

	cli := &dockerClient{
		version:  "1.24",
		scheme:   "http",
		addr:     serverURL,
		basePath: "",
		registry: "registry.example.com",
		client:   server.Client(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := cli.PullImage(ctx, "repo/app", "latest")
	require.Error(err)
	require.Contains(err.Error(), "context canceled")
}
