package blobclient_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hostlist"
	mockblobclient "github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/httputil"
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
			require.Equal(tt.want, got)
		})
	}
}

func TestNewClientResolver(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	provider := mockblobclient.NewMockProvider(ctrl)
	cluster := getMockList("origin1:8080")

	resolver := blobclient.NewClientResolver(provider, cluster)
	require.NotNil(t, resolver)
}

func TestClientResolverResolve(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(t *testing.T) (blobclient.Provider, hostlist.List)
		wantErr     bool
		errContains string
		wantClients int
	}{
		{
			name: "successful resolve returns clients",
			setup: func(t *testing.T) (blobclient.Provider, hostlist.List) {
				addr := testClusterServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Origin-Locations", "origin1:8080,origin2:8080")
					w.WriteHeader(http.StatusOK)
				})

				// Use real provider for integration-like test
				return blobclient.NewProvider(), getMockList(addr)
			},
			wantClients: 2,
			wantErr:     false,
		},
		{
			name: "empty cluster returns error",
			setup: func(t *testing.T) (blobclient.Provider, hostlist.List) {
				return blobclient.NewProvider(), getMockList()
			},
			wantErr:     true,
			errContains: "cluster is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, cluster := tt.setup(t)
			resolver := blobclient.NewClientResolver(provider, cluster)
			d := core.DigestFixture()

			clients, err := resolver.Resolve(d)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			require.Len(t, clients, tt.wantClients)
		})
	}
}

func TestNewClusterClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	resolver := mockblobclient.NewMockClientResolver(ctrl)
	client := blobclient.NewClusterClient(resolver)
	require.NotNil(t, client)
}

func TestClusterClientCheckReadiness(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		{
			name: "successful readiness check",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().CheckReadiness().Return(nil)

				return resolver
			},
			wantErr: false,
		},
		{
			name: "resolve error",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return(nil, errors.New("resolve failed"))
				return resolver
			},
			wantErr:     true,
			errContains: "resolve clients",
		},
		{
			name: "client readiness check fails",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().CheckReadiness().Return(errors.New("not ready"))

				return resolver
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			resolver := tt.setup(ctrl)
			client := blobclient.NewClusterClient(resolver)

			err := client.CheckReadiness()

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestClusterClientUploadBlob(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		{
			name: "successful upload on first client",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return("origin1:8080").AnyTimes()
				client.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

				return resolver
			},
			wantErr: false,
		},
		{
			name: "resolve error",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return(nil, errors.New("resolve failed"))
				return resolver
			},
			wantErr:     true,
			errContains: "resolve clients",
		},
		{
			name: "first client fails with retryable error, second succeeds",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().Addr().Return("origin1:8080").AnyTimes()
				client1.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(
					httputil.StatusError{Status: http.StatusServiceUnavailable})
				client2.EXPECT().Addr().Return("origin2:8080").AnyTimes()
				client2.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

				return resolver
			},
			wantErr: false,
		},
		{
			name: "non-retryable error stops retry",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return("origin1:8080").AnyTimes()
				client.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(
					httputil.StatusError{Status: http.StatusBadRequest})

				return resolver
			},
			wantErr: true,
		},
		{
			name: "all clients fail",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().Addr().Return("origin1:8080").AnyTimes()
				client1.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(
					httputil.StatusError{Status: http.StatusServiceUnavailable})
				client2.EXPECT().Addr().Return("origin2:8080").AnyTimes()
				client2.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(
					httputil.StatusError{Status: http.StatusServiceUnavailable})

				return resolver
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			resolver := tt.setup(ctrl)
			client := blobclient.NewClusterClient(resolver)

			blob := bytes.NewReader([]byte("test blob data"))
			err := client.UploadBlob("test-namespace", core.DigestFixture(), blob)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestClusterClientGetMetaInfo(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		{
			name: "successful get metainfo",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().GetMetaInfo(gomock.Any(), gomock.Any()).Return(&core.MetaInfo{}, nil)

				return resolver
			},
			wantErr: false,
		},
		{
			name: "resolve error",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return(nil, errors.New("resolve failed"))
				return resolver
			},
			wantErr:     true,
			errContains: "resolve clients",
		},
		{
			name: "first client fails, second succeeds",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().GetMetaInfo(gomock.Any(), gomock.Any()).Return(nil, errors.New("failed"))
				client2.EXPECT().GetMetaInfo(gomock.Any(), gomock.Any()).Return(&core.MetaInfo{}, nil)

				return resolver
			},
			wantErr: false,
		},
		{
			name: "202 accepted stops retry to other origins",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().GetMetaInfo(gomock.Any(), gomock.Any()).Return(nil,
					httputil.StatusError{Status: http.StatusAccepted})

				return resolver
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			resolver := tt.setup(ctrl)
			client := blobclient.NewClusterClient(resolver)

			mi, err := client.GetMetaInfo("test-namespace", core.DigestFixture())

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, mi)
		})
	}
}

func TestClusterClientStat(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		{
			name: "successful stat",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Stat(gomock.Any(), gomock.Any()).Return(&core.BlobInfo{Size: 100}, nil)

				return resolver
			},
			wantErr: false,
		},
		{
			name: "resolve error",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return(nil, errors.New("resolve failed"))
				return resolver
			},
			wantErr:     true,
			errContains: "resolve clients",
		},
		{
			name: "first client fails, second succeeds",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				// Due to shuffle, we need to allow either order
				client1.EXPECT().Stat(gomock.Any(), gomock.Any()).Return(nil, errors.New("failed")).AnyTimes()
				client2.EXPECT().Stat(gomock.Any(), gomock.Any()).Return(&core.BlobInfo{Size: 100}, nil).AnyTimes()

				return resolver
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			resolver := tt.setup(ctrl)
			client := blobclient.NewClusterClient(resolver)

			bi, err := client.Stat("test-namespace", core.DigestFixture())

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, bi)
		})
	}
}

func TestClusterClientOverwriteMetaInfo(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		{
			name: "successful overwrite on all clients",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().Addr().Return("origin1:8080").AnyTimes()
				client1.EXPECT().OverwriteMetaInfo(gomock.Any(), gomock.Any()).Return(nil)
				client2.EXPECT().Addr().Return("origin2:8080").AnyTimes()
				client2.EXPECT().OverwriteMetaInfo(gomock.Any(), gomock.Any()).Return(nil)

				return resolver
			},
			wantErr: false,
		},
		{
			name: "resolve error",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return(nil, errors.New("resolve failed"))
				return resolver
			},
			wantErr:     true,
			errContains: "resolve clients",
		},
		{
			name: "one client fails returns error",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().Addr().Return("origin1:8080").AnyTimes()
				client1.EXPECT().OverwriteMetaInfo(gomock.Any(), gomock.Any()).Return(nil)
				client2.EXPECT().Addr().Return("origin2:8080").AnyTimes()
				client2.EXPECT().OverwriteMetaInfo(gomock.Any(), gomock.Any()).Return(errors.New("failed"))

				return resolver
			},
			wantErr:     true,
			errContains: "origin2:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			resolver := tt.setup(ctrl)
			client := blobclient.NewClusterClient(resolver)

			err := client.OverwriteMetaInfo(core.DigestFixture(), 1024)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestClusterClientPrefetchBlob(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		{
			name: "successful prefetch",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().PrefetchBlob(gomock.Any(), gomock.Any()).Return(nil)

				return resolver
			},
			wantErr: false,
		},
		{
			name: "resolve error",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return(nil, errors.New("resolve failed"))
				return resolver
			},
			wantErr:     true,
			errContains: "resolve clients",
		},
		{
			name: "not found error",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().PrefetchBlob(gomock.Any(), gomock.Any()).Return(
					httputil.StatusError{Status: http.StatusNotFound})

				return resolver
			},
			wantErr:     true,
			errContains: "blob not found",
		},
		{
			name: "first client fails, second succeeds",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().PrefetchBlob(gomock.Any(), gomock.Any()).Return(errors.New("unavailable"))
				client2.EXPECT().PrefetchBlob(gomock.Any(), gomock.Any()).Return(nil)

				return resolver
			},
			wantErr: false,
		},
		{
			name: "all clients fail",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().PrefetchBlob(gomock.Any(), gomock.Any()).Return(errors.New("unavailable"))
				client2.EXPECT().PrefetchBlob(gomock.Any(), gomock.Any()).Return(errors.New("unavailable"))

				return resolver
			},
			wantErr:     true,
			errContains: "all origins unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			resolver := tt.setup(ctrl)
			client := blobclient.NewClusterClient(resolver)

			err := client.PrefetchBlob("test-namespace", core.DigestFixture())

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestClusterClientOwners(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
		wantPeers   int
	}{
		{
			name: "successful get owners",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().GetPeerContext().Return(core.PeerContext{PeerID: core.PeerIDFixture()}, nil)
				client2.EXPECT().GetPeerContext().Return(core.PeerContext{PeerID: core.PeerIDFixture()}, nil)

				return resolver
			},
			wantErr:   false,
			wantPeers: 2,
		},
		{
			name: "resolve error",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return(nil, errors.New("resolve failed"))
				return resolver
			},
			wantErr:     true,
			errContains: "resolve clients",
		},
		{
			name: "partial success - one client fails",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().GetPeerContext().Return(core.PeerContext{PeerID: core.PeerIDFixture()}, nil)
				client2.EXPECT().GetPeerContext().Return(core.PeerContext{}, errors.New("failed"))

				return resolver
			},
			wantErr:   false,
			wantPeers: 1,
		},
		{
			name: "all clients fail",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().GetPeerContext().Return(core.PeerContext{}, errors.New("failed"))
				client2.EXPECT().GetPeerContext().Return(core.PeerContext{}, errors.New("failed"))

				return resolver
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			resolver := tt.setup(ctrl)
			client := blobclient.NewClusterClient(resolver)

			peers, err := client.Owners(core.DigestFixture())

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
			require.Len(t, peers, tt.wantPeers)
		})
	}
}

func TestClusterClientDownloadBlob(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		{
			name: "successful download",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return("origin1:8080").AnyTimes()
				client.EXPECT().DownloadBlob(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(namespace string, d core.Digest, dst io.Writer) error {
						dst.Write([]byte("blob data"))
						return nil
					})

				return resolver
			},
			wantErr: false,
		},
		{
			name: "resolve error",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return(nil, errors.New("resolve failed"))
				return resolver
			},
			wantErr:     true,
			errContains: "resolve clients",
		},
		{
			name: "not found returns ErrBlobNotFound",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return("origin1:8080").AnyTimes()
				client.EXPECT().DownloadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(
					httputil.StatusError{Status: http.StatusNotFound})

				return resolver
			},
			wantErr:     true,
			errContains: "blob not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			resolver := tt.setup(ctrl)
			client := blobclient.NewClusterClient(resolver)

			var buf bytes.Buffer
			err := client.DownloadBlob("test-namespace", core.DigestFixture(), &buf)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestClusterClientReplicateToRemote(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		{
			name: "successful replication",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return("origin1:8080").AnyTimes()
				client.EXPECT().ReplicateToRemote(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

				return resolver
			},
			wantErr: false,
		},
		{
			name: "resolve error",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return(nil, errors.New("resolve failed"))
				return resolver
			},
			wantErr:     true,
			errContains: "resolve clients",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			resolver := tt.setup(ctrl)
			client := blobclient.NewClusterClient(resolver)

			err := client.ReplicateToRemote("test-namespace", core.DigestFixture(), "remote.dns.com")

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestPoll(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		makeRequest func(client blobclient.Client) error
		wantErr     bool
		errContains string
	}{
		{
			name: "successful request on first try",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return("origin1:8080").AnyTimes()

				return resolver
			},
			makeRequest: func(client blobclient.Client) error {
				return nil
			},
			wantErr: false,
		},
		{
			name: "resolve error",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return(nil, errors.New("resolve failed"))
				return resolver
			},
			makeRequest: func(client blobclient.Client) error {
				return nil
			},
			wantErr:     true,
			errContains: "resolve clients",
		},
		{
			name: "4xx error does not retry",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return("origin1:8080").AnyTimes()

				return resolver
			},
			makeRequest: func(client blobclient.Client) error {
				return httputil.StatusError{Status: http.StatusNotFound}
			},
			wantErr: true,
		},
		{
			name: "5xx error tries next origin",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)

				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().Addr().Return("origin1:8080").AnyTimes()
				client2.EXPECT().Addr().Return("origin2:8080").AnyTimes()

				return resolver
			},
			makeRequest: func() func(client blobclient.Client) error {
				callCount := 0
				return func(client blobclient.Client) error {
					callCount++
					if callCount == 1 {
						return httputil.StatusError{Status: http.StatusInternalServerError}
					}
					return nil
				}
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			resolver := tt.setup(ctrl)

			// Use a very short backoff for testing
			b := &testBackoff{}
			err := blobclient.Poll(resolver, b, core.DigestFixture(), tt.makeRequest)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

// testBackoff is a simple backoff that never waits
type testBackoff struct {
	maxCalls int
	calls    int
}

func (b *testBackoff) Reset() { b.calls = 0 }
func (b *testBackoff) NextBackOff() time.Duration {
	b.calls++
	if b.maxCalls > 0 && b.calls >= b.maxCalls {
		return -1 // backoff.Stop
	}
	return 0
}

func TestPollWith202Accepted(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	resolver := mockblobclient.NewMockClientResolver(ctrl)
	client := mockblobclient.NewMockClient(ctrl)

	resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
	client.EXPECT().Addr().Return("origin1:8080").AnyTimes()

	callCount := 0
	makeRequest := func(c blobclient.Client) error {
		callCount++
		if callCount < 3 {
			return httputil.StatusError{Status: http.StatusAccepted}
		}
		return nil
	}

	b := &testBackoff{maxCalls: 10}
	err := blobclient.Poll(resolver, b, core.DigestFixture(), makeRequest)

	require.NoError(t, err)
	require.Equal(t, 3, callCount, "should have retried on 202")
}

func TestPollBackoffTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	resolver := mockblobclient.NewMockClientResolver(ctrl)
	client := mockblobclient.NewMockClient(ctrl)

	resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
	client.EXPECT().Addr().Return("origin1:8080").AnyTimes()

	makeRequest := func(c blobclient.Client) error {
		return httputil.StatusError{Status: http.StatusAccepted}
	}

	// Backoff that times out immediately
	b := &testBackoff{maxCalls: 2}
	err := blobclient.Poll(resolver, b, core.DigestFixture(), makeRequest)

	require.Error(t, err)
	require.Contains(t, err.Error(), "backoff timed out")
}

func TestClusterClientUploadBlobNetworkError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	resolver := mockblobclient.NewMockClientResolver(ctrl)
	client1 := mockblobclient.NewMockClient(ctrl)
	client2 := mockblobclient.NewMockClient(ctrl)

	resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
	client1.EXPECT().Addr().Return("origin1:8080").AnyTimes()
	client1.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		httputil.NetworkError{})
	client2.EXPECT().Addr().Return("origin2:8080").AnyTimes()
	client2.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	client := blobclient.NewClusterClient(resolver)
	blob := bytes.NewReader([]byte("test blob data"))
	err := client.UploadBlob("test-namespace", core.DigestFixture(), blob)

	require.NoError(t, err)
}

func TestClusterClientUploadBlobSeekError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	resolver := mockblobclient.NewMockClientResolver(ctrl)
	client1 := mockblobclient.NewMockClient(ctrl)
	client2 := mockblobclient.NewMockClient(ctrl)

	resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
	client1.EXPECT().Addr().Return("origin1:8080").AnyTimes()
	client1.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		httputil.StatusError{Status: http.StatusServiceUnavailable})

	clusterClient := blobclient.NewClusterClient(resolver)

	// Use a reader that fails on Seek
	blob := &failingSeeker{Reader: bytes.NewReader([]byte("test blob data"))}
	err := clusterClient.UploadBlob("test-namespace", core.DigestFixture(), blob)

	require.Error(t, err)
	require.Contains(t, err.Error(), "rewind blob for retry")
}

// failingSeeker is a ReadSeeker that always fails on Seek
type failingSeeker struct {
	*bytes.Reader
}

func (f *failingSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("seek failed")
}

func TestClusterClientStatAllClientsFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	resolver := mockblobclient.NewMockClientResolver(ctrl)
	client := mockblobclient.NewMockClient(ctrl)

	resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
	client.EXPECT().Stat(gomock.Any(), gomock.Any()).Return(nil, errors.New("failed"))

	clusterClient := blobclient.NewClusterClient(resolver)
	bi, err := clusterClient.Stat("test-namespace", core.DigestFixture())

	require.Error(t, err)
	require.Nil(t, bi)
}

func TestClusterClientOwnersNoClientsResolved(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	resolver := mockblobclient.NewMockClientResolver(ctrl)

	// Resolve returns empty clients (shouldn't happen normally but tests edge case)
	resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{}, nil)

	client := blobclient.NewClusterClient(resolver)
	peers, err := client.Owners(core.DigestFixture())

	require.Error(t, err)
	require.Contains(t, err.Error(), "no origin peers found")
	require.Nil(t, peers)
}
