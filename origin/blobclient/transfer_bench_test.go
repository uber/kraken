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
package blobclient_test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/memsize"
)

// connCountListener wraps a net.Listener and counts the number of accepted
// connections.
type connCountListener struct {
	net.Listener
	n int64
}

func (l *connCountListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		atomic.AddInt64(&l.n, 1)
	}
	return c, err
}

func (l *connCountListener) count() int64 { return atomic.LoadInt64(&l.n) }

// fakeUploadServer starts a fake origin server that handles the three endpoints
// used by transferClient: POST (start), PATCH (chunk), PUT (commit). Request
// bodies are discarded.
func fakeUploadServer(b *testing.B, useTLS bool) (addr string, clientTLS *tls.Config, l *connCountListener) {
	b.Helper()

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			io.Copy(io.Discard, r.Body) //nolint:errcheck
		}
		switch r.Method {
		case http.MethodPost:
			w.Header().Set("Location", "bench-uid")
			w.WriteHeader(http.StatusOK)
		case http.MethodPatch, http.MethodPut:
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	l = &connCountListener{Listener: inner}

	s := httptest.NewUnstartedServer(h)
	s.Listener = l

	if useTLS {
		s.StartTLS()
		certPool := x509.NewCertPool()
		certPool.AddCert(s.Certificate())
		clientTLS = &tls.Config{RootCAs: certPool}
		addr = strings.TrimPrefix(s.URL, "https://")
	} else {
		s.Start()
		addr = strings.TrimPrefix(s.URL, "http://")
	}
	b.Cleanup(s.Close)

	return addr, clientTLS, l
}

// BenchmarkTransferBlob measures TCP connection establishment per TransferBlob
// call. The conns/op metric is the server-side Accept() count divided by b.N.
// A value near 0 means keep-alive is working; a value near 6 (1 POST + 4 PATCH
// + 1 PUT for a 4 MB blob at 1 MB chunk size) means every sub-request opens a
// fresh connection.
func BenchmarkTransferBlob(b *testing.B) {
	cases := []struct {
		name   string
		useTLS bool
	}{
		{"http", false},
		{"https_unpooled", true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			addr, clientTLS, l := fakeUploadServer(b, tc.useTLS)

			var opts []blobclient.Option
			opts = append(opts, blobclient.WithChunkSize(1*memsize.MB))
			if tc.useTLS {
				opts = append(opts, blobclient.WithTLS(clientTLS))
			}

			p := blobclient.NewProvider(opts...)
			client := p.Provide(addr)
			blob := make([]byte, 4*memsize.MB)
			digest := core.DigestFixture()
			b.SetBytes(int64(len(blob)))
			for b.Loop() {
				if err := client.TransferBlob(digest, bytes.NewReader(blob)); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportMetric(float64(l.count())/float64(b.N), "conns/op")
		})
	}
}
