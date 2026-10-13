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
	tests := []struct {
		name         string
		host         string
		expectedAddr string
		expectedPath string
		expectedErr  string
	}{
		{
			name:         "valid tcp host",
			host:         "tcp://127.0.0.1:2375",
			expectedAddr: "127.0.0.1:2375",
			expectedPath: "",
		},
		{
			name:         "valid tcp host with path",
			host:         "tcp://127.0.0.1:2375/prefix",
			expectedAddr: "127.0.0.1:2375",
			expectedPath: "/prefix",
		},
		{
			name:         "valid unix host",
			host:         "unix:///var/run/docker.sock",
			expectedAddr: "/var/run/docker.sock",
			expectedPath: "",
		},
		{
			name:        "invalid host missing scheme",
			host:        "invalid-host",
			expectedErr: "unable to parse docker host",
		},
		{
			name:        "unsupported protocol",
			host:        "udp://127.0.0.1:2375",
			expectedErr: "protocol udp not supported",
		},
		{
			name:        "unix socket path too long",
			host:        "unix:///" + strings.Repeat("a", 200),
			expectedErr: "unix socket path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			client, addr, basePath, err := parseHost(tt.host)
			if tt.expectedErr != "" {
				require.Error(err)
				require.Contains(err.Error(), tt.expectedErr)
			} else {
				require.NoError(err)
				require.NotNil(client)
				require.Equal(tt.expectedAddr, addr)
				require.Equal(tt.expectedPath, basePath)
			}
		})
	}
}

func TestNewDockerClient(t *testing.T) {
	tests := []struct {
		name             string
		config           Config
		registry         string
		expectedVersion  string
		expectedScheme   string
		expectedAddr     string
		expectedRegistry string
		expectedErr      bool
	}{
		{
			name: "valid tcp config",
			config: Config{
				DockerHost:          "tcp://127.0.0.1:2375",
				DockerScheme:        "http",
				DockerClientVersion: "1.24",
			},
			registry:         "example.com:5000",
			expectedVersion:  "1.24",
			expectedScheme:   "http",
			expectedAddr:     "127.0.0.1:2375",
			expectedRegistry: "example.com:5000",
		},
		{
			name: "invalid docker host",
			config: Config{
				DockerHost: "invalid-host",
			},
			registry:    "example.com:5000",
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			cli, err := NewDockerClient(tt.config, tt.registry)
			if tt.expectedErr {
				require.Error(err)
				require.Nil(cli)
			} else {
				require.NoError(err)
				require.NotNil(cli)

				dcli, ok := cli.(*dockerClient)
				require.True(ok)
				require.Equal(tt.expectedVersion, dcli.version)
				require.Equal(tt.expectedScheme, dcli.scheme)
				require.Equal(tt.expectedAddr, dcli.addr)
				require.Equal(tt.expectedRegistry, dcli.registry)
			}
		})
	}
}

func TestPullImage(t *testing.T) {
	tests := []struct {
		name          string
		handler       http.HandlerFunc
		ctx           func() (context.Context, context.CancelFunc)
		repo          string
		tag           string
		clientVersion string
		expectedErr   string
	}{
		{
			name: "pull image success",
			handler: func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "POST", r.Method)
				require.Equal(t, "/v1.24/images/create", r.URL.Path)
				require.Equal(t, "", r.Header.Get("X-Registry-Auth"))
				require.Equal(t, "registry.example.com/repo/app", r.URL.Query().Get("fromImage"))
				require.Equal(t, "v1.0.0", r.URL.Query().Get("tag"))
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(`{"status":"downloading"}`))
				require.NoError(t, err)
			},
			repo:          "repo/app",
			tag:           "v1.0.0",
			clientVersion: "1.24",
		},
		{
			name: "pull image 404 error response",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, err := w.Write([]byte("image not found"))
				require.NoError(t, err)
			},
			repo:        "nonexistent",
			tag:         "latest",
			expectedErr: "code 404",
		},
		{
			name: "pull image context canceled",
			handler: func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(100 * time.Millisecond)
				w.WriteHeader(http.StatusOK)
			},
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			repo:          "repo/app",
			tag:           "latest",
			clientVersion: "1.24",
			expectedErr:   "context canceled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			server := httptest.NewServer(tt.handler)
			defer server.Close()

			serverURL := strings.TrimPrefix(server.URL, "http://")

			cli := &dockerClient{
				version:  tt.clientVersion,
				scheme:   "http",
				addr:     serverURL,
				basePath: "",
				registry: "registry.example.com",
				client:   server.Client(),
			}

			ctx := context.Background()
			if tt.ctx != nil {
				var cancel context.CancelFunc
				ctx, cancel = tt.ctx()
				defer cancel()
			}

			err := cli.PullImage(ctx, tt.repo, tt.tag)
			if tt.expectedErr != "" {
				require.Error(err)
				require.Contains(err.Error(), tt.expectedErr)
			} else {
				require.NoError(err)
			}
		})
	}
}
