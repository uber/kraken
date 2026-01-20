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

func TestNew(t *testing.T) {
	t.Run("default values", func(t *testing.T) {
		require := require.New(t)

		client := New("localhost:8080")

		require.Equal("localhost:8080", client.addr)
		require.Equal(uint64(32*memsize.MB), client.chunkSize)
		require.Nil(client.tls)
	})

	t.Run("with custom chunk size", func(t *testing.T) {
		require := require.New(t)

		client := New("localhost:8080", WithChunkSize(64*memsize.MB))

		require.Equal(uint64(64*memsize.MB), client.chunkSize)
	})

	t.Run("with TLS config", func(t *testing.T) {
		require := require.New(t)

		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		client := New("localhost:8080", WithTLS(tlsConfig))

		require.Equal(tlsConfig, client.tls)
	})

	t.Run("with multiple options", func(t *testing.T) {
		require := require.New(t)

		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		client := New("localhost:8080",
			WithChunkSize(16*memsize.MB),
			WithTLS(tlsConfig))

		require.Equal(uint64(16*memsize.MB), client.chunkSize)
		require.Equal(tlsConfig, client.tls)
	})
}

func TestAddr(t *testing.T) {
	require := require.New(t)

	client := New("test-host:9999")

	require.Equal("test-host:9999", client.Addr())
}

func TestCheckReadiness(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal("/readiness", r.URL.Path)
			require.Equal(http.MethodGet, r.Method)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.CheckReadiness()
		require.NoError(err)
	})

	t.Run("not ready", func(t *testing.T) {
		require := require.New(t)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.CheckReadiness()
		require.Error(err)
		require.Contains(err.Error(), "origin not ready")
	})
}

func TestLocations(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		expectedLocations := []string{"origin1:8080", "origin2:8080", "origin3:8080"}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(fmt.Sprintf("/blobs/%s/locations", d), r.URL.Path)
			require.Equal(http.MethodGet, r.Method)

			w.Header().Set("Origin-Locations", strings.Join(expectedLocations, ","))
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		locs, err := client.Locations(d)
		require.NoError(err)
		require.Equal(expectedLocations, locs)
	})

	t.Run("single location", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		expectedLocations := []string{"origin1:8080"}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Origin-Locations", "origin1:8080")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		locs, err := client.Locations(d)
		require.NoError(err)
		require.Equal(expectedLocations, locs)
	})

	t.Run("server error", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		_, err := client.Locations(d)
		require.Error(err)
	})
}

func TestStat(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"
		expectedSize := int64(1024)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(fmt.Sprintf("/internal/namespace/%s/blobs/%s", namespace, d), r.URL.Path)
			require.Equal(http.MethodHead, r.Method)
			require.Empty(r.URL.Query().Get("local"))

			w.Header().Set("Content-Length", fmt.Sprintf("%d", expectedSize))
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		info, err := client.Stat(namespace, d)
		require.NoError(err)
		require.NotNil(info)
		require.Equal(expectedSize, info.Size)
	})

	t.Run("not found returns ErrBlobNotFound", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		_, err := client.Stat(namespace, d)
		require.Equal(ErrBlobNotFound, err)
	})

	t.Run("server error", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		_, err := client.Stat(namespace, d)
		require.Error(err)
		require.NotEqual(ErrBlobNotFound, err)
	})

	t.Run("no content length header", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		info, err := client.Stat(namespace, d)
		require.NoError(err)
		require.NotNil(info)
		require.Equal(int64(0), info.Size)
	})

	t.Run("namespace with special characters", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "namespace/with/slashes"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// URL should have properly escaped namespace
			require.Contains(r.URL.Path, "/internal/namespace/")
			w.Header().Set("Content-Length", "512")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		info, err := client.Stat(namespace, d)
		require.NoError(err)
		require.NotNil(info)
	})

	t.Run("invalid content length header", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		// Use a raw TCP handler to send an invalid Content-Length that Go's stdlib won't catch
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Hijack the connection to send raw response
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("cannot hijack connection")
			}
			conn, buf, _ := hijacker.Hijack()
			defer conn.Close()
			
			// Send raw HTTP response with invalid Content-Length
			buf.WriteString("HTTP/1.1 200 OK\r\n")
			buf.WriteString("Content-Length: not-a-number\r\n")
			buf.WriteString("\r\n")
			buf.Flush()
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		_, err := client.Stat(namespace, d)
		require.Error(err)
	})
}

func TestStatLocal(t *testing.T) {
	t.Run("success with local param", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"
		expectedSize := int64(2048)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(fmt.Sprintf("/internal/namespace/%s/blobs/%s", namespace, d), r.URL.Path)
			require.Equal(http.MethodHead, r.Method)
			require.Equal("true", r.URL.Query().Get("local"))

			w.Header().Set("Content-Length", fmt.Sprintf("%d", expectedSize))
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		info, err := client.StatLocal(namespace, d)
		require.NoError(err)
		require.NotNil(info)
		require.Equal(expectedSize, info.Size)
	})

	t.Run("not found returns ErrBlobNotFound", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal("true", r.URL.Query().Get("local"))
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		_, err := client.StatLocal(namespace, d)
		require.Equal(ErrBlobNotFound, err)
	})
}

func TestDeleteBlob(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(fmt.Sprintf("/internal/blobs/%s", d), r.URL.Path)
			require.Equal(http.MethodDelete, r.Method)

			w.WriteHeader(http.StatusAccepted)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.DeleteBlob(d)
		require.NoError(err)
	})

	t.Run("server error", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.DeleteBlob(d)
		require.Error(err)
	})
}

func TestDownloadBlob(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"
		expectedContent := []byte("test blob content for download")

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(fmt.Sprintf("/namespace/%s/blobs/%s", namespace, d), r.URL.Path)
			require.Equal(http.MethodGet, r.Method)

			w.WriteHeader(http.StatusOK)
			w.Write(expectedContent)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		var buf bytes.Buffer
		err := client.DownloadBlob(namespace, d, &buf)
		require.NoError(err)
		require.Equal(expectedContent, buf.Bytes())
	})

	t.Run("not found", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		var buf bytes.Buffer
		err := client.DownloadBlob(namespace, d, &buf)
		require.Error(err)
	})

	t.Run("accepted status (blob still downloading)", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		var buf bytes.Buffer
		err := client.DownloadBlob(namespace, d, &buf)
		require.Error(err)
	})

	t.Run("large blob download", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"
		largeContent := bytes.Repeat([]byte("x"), 1024*1024) // 1MB

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write(largeContent)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		var buf bytes.Buffer
		err := client.DownloadBlob(namespace, d, &buf)
		require.NoError(err)
		require.Equal(largeContent, buf.Bytes())
	})

	t.Run("write error during copy", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"
		content := []byte("test blob content")

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.DownloadBlob(namespace, d, &errorWriter{})
		require.Error(err)
		require.Contains(err.Error(), "copy body")
	})
}

func TestPrefetchBlob(t *testing.T) {
	t.Run("success with 200", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(fmt.Sprintf("/namespace/%s/blobs/%s/prefetch", namespace, d), r.URL.Path)
			require.Equal(http.MethodPost, r.Method)

			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.PrefetchBlob(namespace, d)
		require.NoError(err)
	})

	t.Run("success with 202 accepted", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.PrefetchBlob(namespace, d)
		require.NoError(err)
	})

	t.Run("server error", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.PrefetchBlob(namespace, d)
		require.Error(err)
	})
}

func TestReplicateToRemote(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"
		remoteDNS := "remote-origin.example.com"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(fmt.Sprintf("/namespace/%s/blobs/%s/remote/%s", namespace, d, remoteDNS), r.URL.Path)
			require.Equal(http.MethodPost, r.Method)

			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.ReplicateToRemote(namespace, d, remoteDNS)
		require.NoError(err)
	})

	t.Run("accepted (blob not ready)", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"
		remoteDNS := "remote-origin.example.com"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.ReplicateToRemote(namespace, d, remoteDNS)
		require.Error(err)
	})
}

func TestGetMetaInfo(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)

		blob := core.NewBlobFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(fmt.Sprintf("/internal/namespace/%s/blobs/%s/metainfo", namespace, blob.Digest), r.URL.Path)
			require.Equal(http.MethodGet, r.Method)

			raw, err := blob.MetaInfo.Serialize()
			require.NoError(err)
			w.WriteHeader(http.StatusOK)
			w.Write(raw)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		mi, err := client.GetMetaInfo(namespace, blob.Digest)
		require.NoError(err)
		require.NotNil(mi)
		require.Equal(blob.MetaInfo.InfoHash(), mi.InfoHash())
		require.Equal(blob.MetaInfo.Length(), mi.Length())
	})

	t.Run("not found", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		_, err := client.GetMetaInfo(namespace, d)
		require.Error(err)
	})

	t.Run("accepted (still downloading)", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		_, err := client.GetMetaInfo(namespace, d)
		require.Error(err)
	})

	t.Run("invalid metainfo response", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("invalid metainfo data"))
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		_, err := client.GetMetaInfo(namespace, d)
		require.Error(err)
		require.Contains(err.Error(), "deserialize metainfo")
	})
}

func TestOverwriteMetaInfo(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		pieceLength := int64(1024 * 1024) // 1MB

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(fmt.Sprintf("/internal/blobs/%s/metainfo", d), r.URL.Path)
			require.Equal(http.MethodPost, r.Method)
			require.Equal(fmt.Sprintf("%d", pieceLength), r.URL.Query().Get("piece_length"))

			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.OverwriteMetaInfo(d, pieceLength)
		require.NoError(err)
	})

	t.Run("server error", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.OverwriteMetaInfo(d, 1024)
		require.Error(err)
	})
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

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal("/internal/peercontext", r.URL.Path)
			require.Equal(http.MethodGet, r.Method)

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(expectedPctx)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		pctx, err := client.GetPeerContext()
		require.NoError(err)
		require.Equal(expectedPctx.IP, pctx.IP)
		require.Equal(expectedPctx.Port, pctx.Port)
		require.Equal(expectedPctx.Zone, pctx.Zone)
		require.Equal(expectedPctx.Cluster, pctx.Cluster)
		require.Equal(expectedPctx.Origin, pctx.Origin)
	})

	t.Run("server error", func(t *testing.T) {
		require := require.New(t)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		_, err := client.GetPeerContext()
		require.Error(err)
	})

	t.Run("invalid JSON response", func(t *testing.T) {
		require := require.New(t)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("invalid json"))
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		_, err := client.GetPeerContext()
		require.Error(err)
	})
}

func TestForceCleanup(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)

		ttl := 24 * time.Hour

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal("/forcecleanup", r.URL.Path)
			require.Equal(http.MethodPost, r.Method)
			require.Equal("24", r.URL.Query().Get("ttl_hr"))

			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.ForceCleanup(ttl)
		require.NoError(err)
	})

	t.Run("fractional TTL rounds up", func(t *testing.T) {
		require := require.New(t)

		ttl := 90 * time.Minute // 1.5 hours should round up to 2

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal("2", r.URL.Query().Get("ttl_hr"))
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.ForceCleanup(ttl)
		require.NoError(err)
	})

	t.Run("server error", func(t *testing.T) {
		require := require.New(t)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.ForceCleanup(time.Hour)
		require.Error(err)
	})
}

func TestTransferBlob(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		content := []byte("test blob content")
		uploadID := "upload-123"

		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++

			switch {
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/uploads"):
				// Start upload
				require.Equal(fmt.Sprintf("/internal/blobs/%s/uploads", d), r.URL.Path)
				w.Header().Set("Location", uploadID)
				w.WriteHeader(http.StatusOK)

			case r.Method == http.MethodPatch:
				// Patch chunk
				require.Contains(r.URL.Path, uploadID)
				require.NotEmpty(r.Header.Get("Content-Range"))
				w.WriteHeader(http.StatusOK)

			case r.Method == http.MethodPut:
				// Commit upload
				require.Contains(r.URL.Path, uploadID)
				w.WriteHeader(http.StatusOK)
			}
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL), WithChunkSize(uint64(len(content)+1)))

		err := client.TransferBlob(d, bytes.NewReader(content))
		require.NoError(err)
		require.Equal(3, requestCount) // start + patch + commit
	})

	t.Run("start upload fails", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		content := []byte("test blob content")

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.TransferBlob(d, bytes.NewReader(content))
		require.Error(err)
	})

	t.Run("missing location header", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		content := []byte("test blob content")

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Don't set Location header
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL))

		err := client.TransferBlob(d, bytes.NewReader(content))
		require.Error(err)
		require.Contains(err.Error(), "Location header not set")
	})

	t.Run("conflict is ignored", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		content := []byte("test blob content")
		uploadID := "upload-123"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost:
				w.Header().Set("Location", uploadID)
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodPatch:
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodPut:
				w.WriteHeader(http.StatusConflict) // Blob already exists
			}
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL), WithChunkSize(uint64(len(content)+1)))

		err := client.TransferBlob(d, bytes.NewReader(content))
		require.NoError(err) // Conflict should be ignored
	})
}

func TestUploadBlob(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		require := require.New(t)

		d := core.DigestFixture()
		namespace := "test-namespace"
		content := []byte("test blob content")
		uploadID := "upload-456"

		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++

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
				// Should be public upload, not duplicate
				require.NotContains(r.URL.Path, "/internal/duplicate/")
				w.WriteHeader(http.StatusOK)
			}
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL), WithChunkSize(uint64(len(content)+1)))

		err := client.UploadBlob(namespace, d, bytes.NewReader(content))
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
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL), WithChunkSize(chunkSize))

		err := client.UploadBlob(namespace, d, bytes.NewReader(content))
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

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/uploads"):
				w.Header().Set("Location", uploadID)
				w.WriteHeader(http.StatusOK)

			case r.Method == http.MethodPatch:
				w.WriteHeader(http.StatusOK)

			case r.Method == http.MethodPut:
				// Should be duplicate upload endpoint
				require.Contains(r.URL.Path, "/internal/duplicate/namespace/")

				// Check request body for delay
				body, _ := io.ReadAll(r.Body)
				var req DuplicateCommitUploadRequest
				err := json.Unmarshal(body, &req)
				require.NoError(err)
				require.Equal(delay, req.Delay)

				w.WriteHeader(http.StatusOK)
			}
		}))
		defer server.Close()

		client := New(stripHTTPPrefix(server.URL), WithChunkSize(uint64(len(content)+1)))

		err := client.DuplicateUploadBlob(namespace, d, bytes.NewReader(content), delay)
		require.NoError(err)
	})
}

// TestClientInterface ensures HTTPClient implements Client interface
func TestClientInterface(t *testing.T) {
	var _ Client = (*HTTPClient)(nil)
}

// errorWriter is a writer that always returns an error
type errorWriter struct{}

func (e *errorWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("write error")
}

// stripHTTPPrefix removes the http:// prefix from a test server URL
// to match the expected address format for the client.
func stripHTTPPrefix(url string) string {
	return strings.TrimPrefix(url, "http://")
}
