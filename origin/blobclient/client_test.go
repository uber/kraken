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
package blobclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/memsize"
)

// testServer creates a test HTTP server and returns a client configured to use it.
// The server is automatically closed when the test completes.
func testServer(t *testing.T, handler http.HandlerFunc, opts ...Option) *HTTPClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return New(stripHTTPPrefix(server.URL), opts...)
}

// statusWithBodyHandler returns a handler that responds with status and body.
func statusWithBodyHandler(t *testing.T, status int, body []byte) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		if _, err := w.Write(body); err != nil {
			t.Fatal(err)
		}
	}
}

// stripHTTPPrefix removes the http:// prefix from a test server URL.
func stripHTTPPrefix(url string) string {
	return strings.TrimPrefix(url, "http://")
}

// errorWriter is a writer that always returns an error.
type errorWriter struct{}

func (e *errorWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("write error")
}

func TestNew(t *testing.T) {
	tests := []struct {
		name          string
		addr          string
		opts          []Option
		wantAddr      string
		wantChunkSize uint64
		wantTLS       bool
	}{
		{
			name:          "default values",
			addr:          "localhost:8080",
			opts:          nil,
			wantAddr:      "localhost:8080",
			wantChunkSize: 32 * memsize.MB,
			wantTLS:       false,
		},
		{
			name:          "with custom chunk size",
			addr:          "localhost:8080",
			opts:          []Option{WithChunkSize(64 * memsize.MB)},
			wantAddr:      "localhost:8080",
			wantChunkSize: 64 * memsize.MB,
			wantTLS:       false,
		},
		{
			name:          "with TLS config",
			addr:          "localhost:8080",
			opts:          []Option{WithTLS(&tls.Config{InsecureSkipVerify: true})},
			wantAddr:      "localhost:8080",
			wantChunkSize: 32 * memsize.MB,
			wantTLS:       true,
		},
		{
			name: "with multiple options",
			addr: "localhost:8080",
			opts: []Option{
				WithChunkSize(16 * memsize.MB),
				WithTLS(&tls.Config{InsecureSkipVerify: true}),
			},
			wantAddr:      "localhost:8080",
			wantChunkSize: 16 * memsize.MB,
			wantTLS:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			client := New(tt.addr, tt.opts...)

			require.Equal(tt.wantAddr, client.addr)
			require.Equal(tt.wantChunkSize, client.chunkSize)
			if tt.wantTLS {
				require.NotNil(client.tls)
			} else {
				require.Nil(client.tls)
			}
		})
	}
}

func TestAddr(t *testing.T) {
	require := require.New(t)
	client := New("test-host:9999")
	require.Equal("test-host:9999", client.Addr())
}

func TestCheckReadiness(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		wantErr    bool
		errContain string
	}{
		{
			name:    "success",
			status:  http.StatusOK,
			wantErr: false,
		},
		{
			name:       "not ready",
			status:     http.StatusServiceUnavailable,
			wantErr:    true,
			errContain: "origin not ready",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal("/readiness", r.URL.Path)
				require.Equal(http.MethodGet, r.Method)
				w.WriteHeader(tt.status)
			})

			err := client.CheckReadiness()
			if tt.wantErr {
				require.Error(err)
				if tt.errContain != "" {
					require.Contains(err.Error(), tt.errContain)
				}
			} else {
				require.NoError(err)
			}
		})
	}
}

func TestLocations(t *testing.T) {
	tests := []struct {
		name          string
		locations     string
		status        int
		wantLocations []string
		wantErr       bool
	}{
		{
			name:          "multiple locations",
			locations:     "origin1:8080,origin2:8080,origin3:8080",
			status:        http.StatusOK,
			wantLocations: []string{"origin1:8080", "origin2:8080", "origin3:8080"},
			wantErr:       false,
		},
		{
			name:          "single location",
			locations:     "origin1:8080",
			status:        http.StatusOK,
			wantLocations: []string{"origin1:8080"},
			wantErr:       false,
		},
		{
			name:    "server error",
			status:  http.StatusInternalServerError,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			d := core.DigestFixture()

			client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(fmt.Sprintf("/blobs/%s/locations", d), r.URL.Path)
				require.Equal(http.MethodGet, r.Method)
				if tt.locations != "" {
					w.Header().Set("Origin-Locations", tt.locations)
				}
				w.WriteHeader(tt.status)
			})

			locs, err := client.Locations(d)
			if tt.wantErr {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.wantLocations, locs)
			}
		})
	}
}

func TestStat(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		contentLength string
		status        int
		wantSize      int64
		wantErr       error
		wantErrMsg    string
	}{
		{
			name:          "success",
			namespace:     "test-namespace",
			contentLength: "1024",
			status:        http.StatusOK,
			wantSize:      1024,
		},
		{
			name:      "not found returns ErrBlobNotFound",
			namespace: "test-namespace",
			status:    http.StatusNotFound,
			wantErr:   ErrBlobNotFound,
		},
		{
			name:       "server error",
			namespace:  "test-namespace",
			status:     http.StatusInternalServerError,
			wantErrMsg: "500",
		},
		{
			name:      "no content length header",
			namespace: "test-namespace",
			status:    http.StatusOK,
			wantSize:  0,
		},
		{
			name:          "namespace with special characters",
			namespace:     "namespace/with/slashes",
			contentLength: "512",
			status:        http.StatusOK,
			wantSize:      512,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			d := core.DigestFixture()

			client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(http.MethodHead, r.Method)
				require.Contains(r.URL.Path, "/internal/namespace/")
				require.Empty(r.URL.Query().Get("local"))

				if tt.contentLength != "" {
					w.Header().Set("Content-Length", tt.contentLength)
				}
				w.WriteHeader(tt.status)
			})

			info, err := client.Stat(tt.namespace, d)
			if tt.wantErr != nil {
				require.Equal(tt.wantErr, err)
			} else if tt.wantErrMsg != "" {
				require.Error(err)
				require.Contains(err.Error(), tt.wantErrMsg)
			} else {
				require.NoError(err)
				require.NotNil(info)
				require.Equal(tt.wantSize, info.Size)
			}
		})
	}

	// Special case: invalid content length header (requires connection hijacking)
	t.Run("invalid content length header", func(t *testing.T) {
		require := require.New(t)
		d := core.DigestFixture()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("cannot hijack connection")
			}
			conn, buf, err := hijacker.Hijack()
			require.NoError(err)
			t.Cleanup(func() {
				require.NoError(conn.Close())
			})

			_, err = buf.WriteString("HTTP/1.1 200 OK\r\n")
			require.NoError(err)
			_, err = buf.WriteString("Content-Length: not-a-number\r\n")
			require.NoError(err)
			_, err = buf.WriteString("\r\n")
			require.NoError(err)
			require.NoError(buf.Flush())
		}))
		t.Cleanup(server.Close)

		client := New(stripHTTPPrefix(server.URL))
		_, err := client.Stat("test-namespace", d)
		require.Error(err)
	})
}

func TestStatLocal(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		size     int64
		wantErr  error
		wantSize int64
	}{
		{
			name:     "success with local param",
			status:   http.StatusOK,
			size:     2048,
			wantSize: 2048,
		},
		{
			name:    "not found returns ErrBlobNotFound",
			status:  http.StatusNotFound,
			wantErr: ErrBlobNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			d := core.DigestFixture()
			namespace := "test-namespace"

			client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(http.MethodHead, r.Method)
				require.Equal("true", r.URL.Query().Get("local"))

				if tt.size > 0 {
					w.Header().Set("Content-Length", fmt.Sprintf("%d", tt.size))
				}
				w.WriteHeader(tt.status)
			})

			info, err := client.StatLocal(namespace, d)
			if tt.wantErr != nil {
				require.Equal(tt.wantErr, err)
			} else {
				require.NoError(err)
				require.NotNil(info)
				require.Equal(tt.wantSize, info.Size)
			}
		})
	}
}

func TestDeleteBlob(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{
			name:    "success",
			status:  http.StatusAccepted,
			wantErr: false,
		},
		{
			name:    "server error",
			status:  http.StatusInternalServerError,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			d := core.DigestFixture()

			client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(fmt.Sprintf("/internal/blobs/%s", d), r.URL.Path)
				require.Equal(http.MethodDelete, r.Method)
				w.WriteHeader(tt.status)
			})

			err := client.DeleteBlob(d)
			if tt.wantErr {
				require.Error(err)
			} else {
				require.NoError(err)
			}
		})
	}
}

func TestDownloadBlob(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		content     []byte
		useErrWrite bool
		wantErr     bool
		errContain  string
	}{
		{
			name:    "success",
			status:  http.StatusOK,
			content: []byte("test blob content for download"),
			wantErr: false,
		},
		{
			name:    "not found",
			status:  http.StatusNotFound,
			wantErr: true,
		},
		{
			name:    "accepted status (blob still downloading)",
			status:  http.StatusAccepted,
			wantErr: true,
		},
		{
			name:    "large blob download",
			status:  http.StatusOK,
			content: bytes.Repeat([]byte("x"), 1024*1024), // 1MB
			wantErr: false,
		},
		{
			name:        "write error during copy",
			status:      http.StatusOK,
			content:     []byte("test blob content"),
			useErrWrite: true,
			wantErr:     true,
			errContain:  "copy body",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			d := core.DigestFixture()
			namespace := "test-namespace"

			client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(fmt.Sprintf("/namespace/%s/blobs/%s", namespace, d), r.URL.Path)
				require.Equal(http.MethodGet, r.Method)
				w.WriteHeader(tt.status)
				if tt.content != nil {
					_, err := w.Write(tt.content)
					require.NoError(err)
				}
			})

			var dst io.Writer
			var buf bytes.Buffer
			if tt.useErrWrite {
				dst = &errorWriter{}
			} else {
				dst = &buf
			}

			err := client.DownloadBlob(namespace, d, dst)
			if tt.wantErr {
				require.Error(err)
				if tt.errContain != "" {
					require.Contains(err.Error(), tt.errContain)
				}
			} else {
				require.NoError(err)
				require.Equal(tt.content, buf.Bytes())
			}
		})
	}
}

func TestPrefetchBlob(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{
			name:    "success with 200",
			status:  http.StatusOK,
			wantErr: false,
		},
		{
			name:    "success with 202 accepted",
			status:  http.StatusAccepted,
			wantErr: false,
		},
		{
			name:    "server error",
			status:  http.StatusInternalServerError,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			d := core.DigestFixture()
			namespace := "test-namespace"

			client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(fmt.Sprintf("/namespace/%s/blobs/%s/prefetch", namespace, d), r.URL.Path)
				require.Equal(http.MethodPost, r.Method)
				w.WriteHeader(tt.status)
			})

			err := client.PrefetchBlob(namespace, d)
			if tt.wantErr {
				require.Error(err)
			} else {
				require.NoError(err)
			}
		})
	}
}

func TestReplicateToRemote(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{
			name:    "success",
			status:  http.StatusOK,
			wantErr: false,
		},
		{
			name:    "accepted (blob not ready)",
			status:  http.StatusAccepted,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			d := core.DigestFixture()
			namespace := "test-namespace"
			remoteDNS := "remote-origin.example.com"

			client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(fmt.Sprintf("/namespace/%s/blobs/%s/remote/%s", namespace, d, remoteDNS), r.URL.Path)
				require.Equal(http.MethodPost, r.Method)
				w.WriteHeader(tt.status)
			})

			err := client.ReplicateToRemote(namespace, d, remoteDNS)
			if tt.wantErr {
				require.Error(err)
			} else {
				require.NoError(err)
			}
		})
	}
}

func TestGetMetaInfo(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)
		blob := core.NewBlobFixture()
		namespace := "test-namespace"

		client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(fmt.Sprintf("/internal/namespace/%s/blobs/%s/metainfo", namespace, blob.Digest), r.URL.Path)
			require.Equal(http.MethodGet, r.Method)

			raw, err := blob.MetaInfo.Serialize()
			require.NoError(err)
			w.WriteHeader(http.StatusOK)
			_, err = w.Write(raw)
			require.NoError(err)
		})

		mi, err := client.GetMetaInfo(namespace, blob.Digest)
		require.NoError(err)
		require.NotNil(mi)
		require.Equal(blob.MetaInfo.InfoHash(), mi.InfoHash())
		require.Equal(blob.MetaInfo.Length(), mi.Length())
	})

	errorTests := []struct {
		name       string
		status     int
		body       []byte
		errContain string
	}{
		{
			name:   "not found",
			status: http.StatusNotFound,
		},
		{
			name:   "accepted (still downloading)",
			status: http.StatusAccepted,
		},
		{
			name:       "invalid metainfo response",
			status:     http.StatusOK,
			body:       []byte("invalid metainfo data"),
			errContain: "deserialize metainfo",
		},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			d := core.DigestFixture()
			namespace := "test-namespace"

			client := testServer(t, statusWithBodyHandler(t, tt.status, tt.body))

			_, err := client.GetMetaInfo(namespace, d)
			require.Error(err)
			if tt.errContain != "" {
				require.Contains(err.Error(), tt.errContain)
			}
		})
	}
}

func TestOverwriteMetaInfo(t *testing.T) {
	tests := []struct {
		name        string
		pieceLength int64
		status      int
		wantErr     bool
	}{
		{
			name:        "success",
			pieceLength: 1024 * 1024,
			status:      http.StatusOK,
			wantErr:     false,
		},
		{
			name:        "server error",
			pieceLength: 1024,
			status:      http.StatusInternalServerError,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			d := core.DigestFixture()

			client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(fmt.Sprintf("/internal/blobs/%s/metainfo", d), r.URL.Path)
				require.Equal(http.MethodPost, r.Method)
				require.Equal(fmt.Sprintf("%d", tt.pieceLength), r.URL.Query().Get("piece_length"))
				w.WriteHeader(tt.status)
			})

			err := client.OverwriteMetaInfo(d, tt.pieceLength)
			if tt.wantErr {
				require.Error(err)
			} else {
				require.NoError(err)
			}
		})
	}
}

func TestGetPeerContext(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)
		expectedPctx := core.PeerContext{
			IP:      "192.168.1.100",
			Port:    8080,
			PeerID:  core.PeerIDFixture(),
			Zone:    "zone1",
			Cluster: "cluster1",
			Origin:  true,
		}

		client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal("/internal/peercontext", r.URL.Path)
			require.Equal(http.MethodGet, r.Method)
			w.WriteHeader(http.StatusOK)
			require.NoError(json.NewEncoder(w).Encode(expectedPctx))
		})

		pctx, err := client.GetPeerContext()
		require.NoError(err)
		require.Equal(expectedPctx.IP, pctx.IP)
		require.Equal(expectedPctx.Port, pctx.Port)
		require.Equal(expectedPctx.Zone, pctx.Zone)
		require.Equal(expectedPctx.Cluster, pctx.Cluster)
		require.Equal(expectedPctx.Origin, pctx.Origin)
	})

	errorTests := []struct {
		name   string
		status int
		body   []byte
	}{
		{
			name:   "server error",
			status: http.StatusInternalServerError,
		},
		{
			name:   "invalid JSON response",
			status: http.StatusOK,
			body:   []byte("invalid json"),
		},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			client := testServer(t, statusWithBodyHandler(t, tt.status, tt.body))

			_, err := client.GetPeerContext()
			require.Error(err)
		})
	}
}

func TestForceCleanup(t *testing.T) {
	tests := []struct {
		name      string
		ttl       time.Duration
		wantTTLHr string
		status    int
		wantErr   bool
	}{
		{
			name:      "success",
			ttl:       24 * time.Hour,
			wantTTLHr: "24",
			status:    http.StatusOK,
			wantErr:   false,
		},
		{
			name:      "fractional TTL rounds up",
			ttl:       90 * time.Minute,
			wantTTLHr: "2",
			status:    http.StatusOK,
			wantErr:   false,
		},
		{
			name:      "server error",
			ttl:       time.Hour,
			wantTTLHr: "1",
			status:    http.StatusInternalServerError,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal("/forcecleanup", r.URL.Path)
				require.Equal(http.MethodPost, r.Method)
				require.Equal(tt.wantTTLHr, r.URL.Query().Get("ttl_hr"))
				w.WriteHeader(tt.status)
			})

			err := client.ForceCleanup(tt.ttl)
			if tt.wantErr {
				require.Error(err)
			} else {
				require.NoError(err)
			}
		})
	}
}

func TestTransferBlob(t *testing.T) {
	tests := []struct {
		name             string
		content          []byte
		uploadID         string
		startStatus      int
		patchStatus      int
		commitStatus     int
		setLocation      bool
		wantErr          bool
		errContain       string
		wantRequestCount int
	}{
		{
			name:             "success",
			content:          []byte("test blob content"),
			uploadID:         "upload-123",
			startStatus:      http.StatusOK,
			patchStatus:      http.StatusOK,
			commitStatus:     http.StatusOK,
			setLocation:      true,
			wantErr:          false,
			wantRequestCount: 3,
		},
		{
			name:        "start upload fails",
			content:     []byte("test blob content"),
			startStatus: http.StatusInternalServerError,
			wantErr:     true,
		},
		{
			name:        "missing location header",
			content:     []byte("test blob content"),
			startStatus: http.StatusOK,
			setLocation: false,
			wantErr:     true,
			errContain:  "Location header not set",
		},
		{
			name:         "conflict is ignored",
			content:      []byte("test blob content"),
			uploadID:     "upload-123",
			startStatus:  http.StatusOK,
			patchStatus:  http.StatusOK,
			commitStatus: http.StatusConflict,
			setLocation:  true,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			d := core.DigestFixture()
			requestCount := 0

			client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
				requestCount++

				switch {
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/uploads"):
					if tt.setLocation {
						w.Header().Set("Location", tt.uploadID)
					}
					w.WriteHeader(tt.startStatus)

				case r.Method == http.MethodPatch:
					require.Contains(r.URL.Path, tt.uploadID)
					require.NotEmpty(r.Header.Get("Content-Range"))
					w.WriteHeader(tt.patchStatus)

				case r.Method == http.MethodPut:
					require.Contains(r.URL.Path, tt.uploadID)
					w.WriteHeader(tt.commitStatus)
				}
			}, WithChunkSize(uint64(len(tt.content)+1)))

			err := client.TransferBlob(d, bytes.NewReader(tt.content))
			if tt.wantErr {
				require.Error(err)
				if tt.errContain != "" {
					require.Contains(err.Error(), tt.errContain)
				}
			} else {
				require.NoError(err)
				if tt.wantRequestCount > 0 {
					require.Equal(tt.wantRequestCount, requestCount)
				}
			}
		})
	}
}

func TestUploadBlob(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)
		d := core.DigestFixture()
		namespace := "test-namespace"
		content := []byte("test blob content")
		uploadID := "upload-456"

		client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/uploads"):
				require.Contains(r.URL.Path, fmt.Sprintf("/namespace/%s/blobs/%s/uploads", namespace, d))
				w.Header().Set("Location", uploadID)
				w.WriteHeader(http.StatusOK)

			case r.Method == http.MethodPatch:
				require.Contains(r.URL.Path, namespace)
				require.Contains(r.URL.Path, uploadID)
				w.WriteHeader(http.StatusOK)

			case r.Method == http.MethodPut:
				require.Contains(r.URL.Path, namespace)
				require.Contains(r.URL.Path, uploadID)
				require.NotContains(r.URL.Path, "/internal/duplicate/")
				w.WriteHeader(http.StatusOK)
			}
		}, WithChunkSize(uint64(len(content)+1)))

		err := client.UploadBlob(context.Background(), namespace, d, bytes.NewReader(content))
		require.NoError(err)
	})

	t.Run("chunked upload with multiple chunks", func(t *testing.T) {
		require := require.New(t)
		d := core.DigestFixture()
		namespace := "test-namespace"
		content := []byte("test blob content that is longer than chunk size")
		uploadID := "upload-789"
		chunkSize := uint64(10)
		patchCount := 0

		client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/uploads"):
				w.Header().Set("Location", uploadID)
				w.WriteHeader(http.StatusOK)

			case r.Method == http.MethodPatch:
				patchCount++
				w.WriteHeader(http.StatusOK)

			case r.Method == http.MethodPut:
				w.WriteHeader(http.StatusOK)
			}
		}, WithChunkSize(chunkSize))

		err := client.UploadBlob(context.Background(), namespace, d, bytes.NewReader(content))
		require.NoError(err)
		require.Equal(5, patchCount) // 49 bytes / 10 bytes per chunk = 5 chunks
	})
}

func TestDuplicateUploadBlob(t *testing.T) {
	t.Run("success with delay", func(t *testing.T) {
		require := require.New(t)
		d := core.DigestFixture()
		namespace := "test-namespace"
		content := []byte("test blob content")
		uploadID := "upload-dup"
		delay := 5 * time.Minute

		client := testServer(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/uploads"):
				w.Header().Set("Location", uploadID)
				w.WriteHeader(http.StatusOK)

			case r.Method == http.MethodPatch:
				w.WriteHeader(http.StatusOK)

			case r.Method == http.MethodPut:
				require.Contains(r.URL.Path, "/internal/duplicate/namespace/")

				body, err := io.ReadAll(r.Body)
				require.NoError(err)
				var req DuplicateCommitUploadRequest
				err = json.Unmarshal(body, &req)
				require.NoError(err)
				require.Equal(delay, req.Delay)

				w.WriteHeader(http.StatusOK)
			}
		}, WithChunkSize(uint64(len(content)+1)))

		err := client.DuplicateUploadBlob(namespace, d, bytes.NewReader(content), delay)
		require.NoError(err)
	})
}

// TestClientInterface ensures HTTPClient implements Client interface
func TestClientInterface(t *testing.T) {
	var _ Client = (*HTTPClient)(nil)
}
