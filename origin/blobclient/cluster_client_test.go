package blobclient_test

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/stringset"
)

type mockList struct {
	addrs []string
}

func (m mockList) Resolve() stringset.Set {
	return stringset.New(m.addrs...)
}

func getMockList(addrs ...string) hostlist.List {
	return mockList{addrs}
}

// stripHTTPPrefix removes the http:// prefix from a URL.
func stripHTTPPrefix(url string) string {
	return strings.TrimPrefix(url, "http://")
}

// testClusterServer creates a test HTTP server and returns the stripped address.
func testClusterServer(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return stripHTTPPrefix(server.URL)
}

func TestClusterLocations(t *testing.T) {
	tests := []struct {
		name         string
		setupServers func(t *testing.T) []string // returns server addresses for cluster
		want         []string
		wantErr      bool
		errContains  string
	}{
		{
			name: "empty cluster",
			setupServers: func(t *testing.T) []string {
				return []string{} // no servers
			},
			wantErr:     true,
			errContains: "cluster is empty",
		},
		{
			name: "single node cluster returns locations",
			setupServers: func(t *testing.T) []string {
				addr := testClusterServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Origin-Locations", "origin1:8080,origin2:8080")
					w.WriteHeader(http.StatusOK)
				})
				return []string{addr}
			},
			want:    []string{"origin1:8080", "origin2:8080"},
			wantErr: false,
		},
		{
			name: "multiple nodes - first succeeds",
			setupServers: func(t *testing.T) []string {
				addr1 := testClusterServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Origin-Locations", "origin1:8080")
					w.WriteHeader(http.StatusOK)
				})
				addr2 := testClusterServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				})
				return []string{addr1, addr2}
			},
			want:    []string{"origin1:8080"},
			wantErr: false,
		},
		{
			name: "first node fails - second succeeds",
			setupServers: func(t *testing.T) []string {
				addr1 := testClusterServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				})
				addr2 := testClusterServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Origin-Locations", "origin2:8080")
					w.WriteHeader(http.StatusOK)
				})
				return []string{addr1, addr2}
			},
			want:    []string{"origin2:8080"},
			wantErr: false,
		},
		{
			name: "all nodes fail",
			setupServers: func(t *testing.T) []string {
				addr1 := testClusterServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				})
				addr2 := testClusterServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				})
				return []string{addr1, addr2}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			addrs := tt.setupServers(t)
			p := blobclient.NewProvider()
			cluster := getMockList(addrs...)
			d := core.DigestFixture()

			got, err := blobclient.Locations(p, cluster, d)

			if tt.wantErr {
				require.Error(err)
				if tt.errContains != "" {
					require.Contains(err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(err)
			require.True(reflect.DeepEqual(got, tt.want), "got %v, want %v", got, tt.want)
		})
	}
}
