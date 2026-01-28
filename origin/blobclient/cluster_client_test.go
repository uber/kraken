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

// =============================================================================
// Constants
// =============================================================================

const (
	testNamespace = "test-namespace"
	testOrigin1   = "origin1:8080"
	testOrigin2   = "origin2:8080"
	testRemoteDNS = "remote.dns.com"
)

// =============================================================================
// Test Helpers
// =============================================================================

// mockList implements hostlist.List for testing cluster operations.
type mockList struct {
	addrs []string
}

func (m mockList) Resolve() stringset.Set {
	return stringset.New(m.addrs...)
}

// getMockList creates a hostlist.List with the given addresses for testing.
func getMockList(addrs ...string) hostlist.List {
	return mockList{addrs}
}

func stripHTTPPrefix(url string) string {
	return strings.TrimPrefix(url, "http://")
}

// testClusterServer creates a test HTTP server and returns its address (without http://).
// The server is automatically cleaned up when the test completes.
func testClusterServer(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return stripHTTPPrefix(server.URL)
}

// testBackoff is a simple backoff for testing that never waits.
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

// failingSeeker is a ReadSeeker that always fails on Seek.
// Used to test error handling when blob data cannot be rewound for retry.
type failingSeeker struct {
	*bytes.Reader
}

func (f *failingSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("seek failed")
}

// =============================================================================
// Mock Setup Helpers
// =============================================================================

// setupResolverError creates a MockClientResolver that always fails to resolve.
func setupResolverError(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
	resolver := mockblobclient.NewMockClientResolver(ctrl)
	resolver.EXPECT().Resolve(gomock.Any()).Return(nil, errors.New("resolve failed"))
	return resolver
}

// runClusterClientTest is a generic test runner for ClusterClient methods.
// It handles common setup/teardown and error checking logic to reduce boilerplate.
func runClusterClientTest(t *testing.T, setup func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver,
	testFn func(client blobclient.ClusterClient) error, wantErr bool, errContains string) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	resolver := setup(ctrl)
	client := blobclient.NewClusterClient(resolver)

	err := testFn(client)

	if wantErr {
		require.Error(t, err)
		if errContains != "" {
			require.Contains(t, err.Error(), errContains)
		}
		return
	}
	require.NoError(t, err)
}

// =============================================================================
// Tests
// =============================================================================

// TestClusterLocations verifies that Locations correctly discovers which origin
// servers own a given blob by querying cluster members and handling failover.
func TestClusterLocations(t *testing.T) {
	tests := []struct {
		name         string
		setupServers func(t *testing.T) []string
		want         []string
		wantErr      bool
		errContains  string
	}{
		// --- Error Cases ---
		{
			name:         "empty cluster",
			setupServers: func(t *testing.T) []string { return []string{} },
			wantErr:      true,
			errContains:  "cluster is empty",
		},

		// --- Success Cases ---
		{
			name: "single node cluster returns locations",
			setupServers: func(t *testing.T) []string {
				return []string{testClusterServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Origin-Locations", "origin1:8080,origin2:8080")
					w.WriteHeader(http.StatusOK)
				})}
			},
			want: []string{"origin1:8080", "origin2:8080"},
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
			want: []string{testOrigin1},
		},

		// --- Failover Cases ---
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
			want: []string{testOrigin2},
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
			addrs := tt.setupServers(t)
			got, err := blobclient.Locations(blobclient.NewProvider(), getMockList(addrs...), core.DigestFixture())

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestNewClientResolver verifies that NewClientResolver creates a valid resolver.
func TestNewClientResolver(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	resolver := blobclient.NewClientResolver(mockblobclient.NewMockProvider(ctrl), getMockList(testOrigin1))
	require.NotNil(t, resolver)
}

// TestClientResolverResolve verifies that ClientResolver correctly resolves
// blob locations to a list of origin clients.
func TestClientResolverResolve(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(t *testing.T) (blobclient.Provider, hostlist.List)
		wantClients int
		wantErr     bool
		errContains string
	}{
		// --- Success Cases ---
		{
			name: "successful resolve returns clients",
			setup: func(t *testing.T) (blobclient.Provider, hostlist.List) {
				addr := testClusterServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Origin-Locations", "origin1:8080,origin2:8080")
					w.WriteHeader(http.StatusOK)
				})
				return blobclient.NewProvider(), getMockList(addr)
			},
			wantClients: 2,
		},

		// --- Error Cases ---
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
			clients, err := blobclient.NewClientResolver(provider, cluster).Resolve(core.DigestFixture())

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

// TestNewClusterClient verifies that NewClusterClient creates a valid cluster client.
func TestNewClusterClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	require.NotNil(t, blobclient.NewClusterClient(mockblobclient.NewMockClientResolver(ctrl)))
}

// TestClusterClientCheckReadiness verifies that CheckReadiness correctly
// determines if the origin cluster is ready to serve requests.
func TestClusterClientCheckReadiness(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		// --- Success Cases ---
		{
			name: "successful readiness check",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().CheckReadiness().Return(nil)
				return resolver
			},
		},

		// --- Error Cases ---
		{
			name:        "resolve error",
			setup:       setupResolverError,
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
			runClusterClientTest(t, tt.setup, func(c blobclient.ClusterClient) error {
				return c.CheckReadiness()
			}, tt.wantErr, tt.errContains)
		})
	}
}

// TestClusterClientUploadBlob verifies that UploadBlob correctly uploads blobs
// to origin servers with proper retry and failover behavior.
func TestClusterClientUploadBlob(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		blob        func() io.ReadSeeker
		wantErr     bool
		errContains string
	}{
		// --- Success Cases ---
		{
			name: "successful upload on first client",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				client.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				return resolver
			},
			blob: func() io.ReadSeeker { return bytes.NewReader([]byte("test data")) },
		},

		// --- Error Cases ---
		{
			name:        "resolve error",
			setup:       setupResolverError,
			blob:        func() io.ReadSeeker { return bytes.NewReader([]byte("test data")) },
			wantErr:     true,
			errContains: "resolve clients",
		},

		// --- Failover Cases ---
		{
			name: "first client fails with retryable error, second succeeds",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				client1.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(
					httputil.StatusError{Status: http.StatusServiceUnavailable})
				client2.EXPECT().Addr().Return(testOrigin2).AnyTimes()
				client2.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				return resolver
			},
			blob: func() io.ReadSeeker { return bytes.NewReader([]byte("test data")) },
		},
		{
			name: "non-retryable error stops retry",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				client.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(
					httputil.StatusError{Status: http.StatusBadRequest})
				return resolver
			},
			blob:    func() io.ReadSeeker { return bytes.NewReader([]byte("test data")) },
			wantErr: true,
		},
		{
			name: "all clients fail",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				client1.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(
					httputil.StatusError{Status: http.StatusServiceUnavailable})
				client2.EXPECT().Addr().Return(testOrigin2).AnyTimes()
				client2.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(
					httputil.StatusError{Status: http.StatusServiceUnavailable})
				return resolver
			},
			blob:    func() io.ReadSeeker { return bytes.NewReader([]byte("test data")) },
			wantErr: true,
		},
		{
			name: "network error retries on next client",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				client1.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(httputil.NetworkError{})
				client2.EXPECT().Addr().Return(testOrigin2).AnyTimes()
				client2.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				return resolver
			},
			blob: func() io.ReadSeeker { return bytes.NewReader([]byte("test data")) },
		},

		// --- Edge Cases ---
		{
			name: "seek error on retry",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, mockblobclient.NewMockClient(ctrl)}, nil)
				client1.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				client1.EXPECT().UploadBlob(gomock.Any(), gomock.Any(), gomock.Any()).Return(
					httputil.StatusError{Status: http.StatusServiceUnavailable})
				return resolver
			},
			blob:        func() io.ReadSeeker { return &failingSeeker{Reader: bytes.NewReader([]byte("test data"))} },
			wantErr:     true,
			errContains: "rewind blob for retry",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			resolver := tt.setup(ctrl)
			client := blobclient.NewClusterClient(resolver)
			err := client.UploadBlob(testNamespace, core.DigestFixture(), tt.blob())

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

// TestClusterClientGetMetaInfo verifies that GetMetaInfo correctly retrieves
// blob metadata from origin servers with proper failover.
func TestClusterClientGetMetaInfo(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		// --- Success Cases ---
		{
			name: "successful get metainfo",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().GetMetaInfo(gomock.Any(), gomock.Any()).Return(&core.MetaInfo{}, nil)
				return resolver
			},
		},

		// --- Error Cases ---
		{
			name:        "resolve error",
			setup:       setupResolverError,
			wantErr:     true,
			errContains: "resolve clients",
		},

		// --- Failover Cases ---
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
		},

		// --- Special Status Codes ---
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
		mi, err := client.GetMetaInfo(testNamespace, core.DigestFixture())
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

// TestClusterClientStat verifies that Stat correctly retrieves blob info
// from origin servers with shuffled client order for load balancing.
func TestClusterClientStat(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		// --- Success Cases ---
		{
			name: "successful stat",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Stat(gomock.Any(), gomock.Any()).Return(&core.BlobInfo{Size: 100}, nil)
				return resolver
			},
		},

		// --- Error Cases ---
		{
			name:        "resolve error",
			setup:       setupResolverError,
			wantErr:     true,
			errContains: "resolve clients",
		},

		// --- Failover Cases ---
		{
			name: "first client fails, second succeeds (shuffle)",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().Stat(gomock.Any(), gomock.Any()).Return(nil, errors.New("failed")).AnyTimes()
				client2.EXPECT().Stat(gomock.Any(), gomock.Any()).Return(&core.BlobInfo{Size: 100}, nil).AnyTimes()
				return resolver
			},
		},
		{
			name: "all clients fail",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Stat(gomock.Any(), gomock.Any()).Return(nil, errors.New("failed"))
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
		bi, err := client.Stat(testNamespace, core.DigestFixture())
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

// TestClusterClientOverwriteMetaInfo verifies that OverwriteMetaInfo correctly
// updates metadata on ALL origin servers that own the blob.
func TestClusterClientOverwriteMetaInfo(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		// --- Success Cases ---
		{
			name: "successful overwrite on all clients",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				client1.EXPECT().OverwriteMetaInfo(gomock.Any(), gomock.Any()).Return(nil)
				client2.EXPECT().Addr().Return(testOrigin2).AnyTimes()
				client2.EXPECT().OverwriteMetaInfo(gomock.Any(), gomock.Any()).Return(nil)
				return resolver
			},
		},

		// --- Error Cases ---
		{
			name:        "resolve error",
			setup:       setupResolverError,
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
				client1.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				client1.EXPECT().OverwriteMetaInfo(gomock.Any(), gomock.Any()).Return(nil)
				client2.EXPECT().Addr().Return(testOrigin2).AnyTimes()
				client2.EXPECT().OverwriteMetaInfo(gomock.Any(), gomock.Any()).Return(errors.New("failed"))
				return resolver
			},
			wantErr:     true,
			errContains: testOrigin2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runClusterClientTest(t, tt.setup, func(c blobclient.ClusterClient) error {
				return c.OverwriteMetaInfo(core.DigestFixture(), 1024)
			}, tt.wantErr, tt.errContains)
		})
	}
}

// TestClusterClientPrefetchBlob verifies that PrefetchBlob correctly triggers
// blob prefetching on origin servers with proper failover.
func TestClusterClientPrefetchBlob(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		// --- Success Cases ---
		{
			name: "successful prefetch",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().PrefetchBlob(gomock.Any(), gomock.Any()).Return(nil)
				return resolver
			},
		},

		// --- Error Cases ---
		{
			name:        "resolve error",
			setup:       setupResolverError,
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

		// --- Failover Cases ---
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
			runClusterClientTest(t, tt.setup, func(c blobclient.ClusterClient) error {
				return c.PrefetchBlob(testNamespace, core.DigestFixture())
			}, tt.wantErr, tt.errContains)
		})
	}
}

// TestClusterClientOwners verifies that Owners correctly retrieves the list
// of origin peers that own a given blob.
func TestClusterClientOwners(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantPeers   int
		wantErr     bool
		errContains string
	}{
		// --- Success Cases ---
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
			wantPeers: 2,
		},

		// --- Error Cases ---
		{
			name:        "resolve error",
			setup:       setupResolverError,
			wantErr:     true,
			errContains: "resolve clients",
		},

		// --- Partial Success Cases ---
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
		{
			name: "no clients resolved",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{}, nil)
				return resolver
			},
			wantErr:     true,
			errContains: "no origin peers found",
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

// TestClusterClientDownloadBlob verifies that DownloadBlob correctly downloads
// blob data from origin servers with proper error handling.
func TestClusterClientDownloadBlob(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		// --- Success Cases ---
		{
			name: "successful download",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				client.EXPECT().DownloadBlob(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(namespace string, d core.Digest, dst io.Writer) error {
						dst.Write([]byte("blob data"))
						return nil
					})
				return resolver
			},
		},

		// --- Error Cases ---
		{
			name:        "resolve error",
			setup:       setupResolverError,
			wantErr:     true,
			errContains: "resolve clients",
		},
		{
			name: "not found returns ErrBlobNotFound",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return(testOrigin1).AnyTimes()
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
			err := client.DownloadBlob(testNamespace, core.DigestFixture(), &buf)

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

// TestClusterClientReplicateToRemote verifies that ReplicateToRemote correctly
// triggers blob replication to a remote cluster.
func TestClusterClientReplicateToRemote(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		wantErr     bool
		errContains string
	}{
		// --- Success Cases ---
		{
			name: "successful replication",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				client.EXPECT().ReplicateToRemote(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				return resolver
			},
		},

		// --- Error Cases ---
		{
			name:        "resolve error",
			setup:       setupResolverError,
			wantErr:     true,
			errContains: "resolve clients",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runClusterClientTest(t, tt.setup, func(c blobclient.ClusterClient) error {
				return c.ReplicateToRemote(testNamespace, core.DigestFixture(), testRemoteDNS)
			}, tt.wantErr, tt.errContains)
		})
	}
}

// TestPoll verifies that Poll correctly handles request polling with backoff,
// retry logic for different HTTP status codes, and failover to other origins.
func TestPoll(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver
		makeRequest func(client blobclient.Client) error
		backoff     *testBackoff
		wantErr     bool
		errContains string
	}{
		// --- Success Cases ---
		{
			name: "successful request on first try",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				return resolver
			},
			makeRequest: func(client blobclient.Client) error { return nil },
			backoff:     &testBackoff{},
		},

		// --- Error Cases ---
		{
			name:        "resolve error",
			setup:       setupResolverError,
			makeRequest: func(client blobclient.Client) error { return nil },
			backoff:     &testBackoff{},
			wantErr:     true,
			errContains: "resolve clients",
		},
		{
			name: "4xx error does not retry",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				return resolver
			},
			makeRequest: func(client blobclient.Client) error {
				return httputil.StatusError{Status: http.StatusNotFound}
			},
			backoff: &testBackoff{},
			wantErr: true,
		},

		// --- Retry/Failover Cases ---
		{
			name: "5xx error tries next origin",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client1 := mockblobclient.NewMockClient(ctrl)
				client2 := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client1, client2}, nil)
				client1.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				client2.EXPECT().Addr().Return(testOrigin2).AnyTimes()
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
			backoff: &testBackoff{},
		},

		// --- 202 Accepted (Polling) Cases ---
		{
			name: "202 accepted retries until success",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				return resolver
			},
			makeRequest: func() func(client blobclient.Client) error {
				callCount := 0
				return func(client blobclient.Client) error {
					callCount++
					if callCount < 3 {
						return httputil.StatusError{Status: http.StatusAccepted}
					}
					return nil
				}
			}(),
			backoff: &testBackoff{maxCalls: 10},
		},
		{
			name: "backoff timeout on 202",
			setup: func(ctrl *gomock.Controller) *mockblobclient.MockClientResolver {
				resolver := mockblobclient.NewMockClientResolver(ctrl)
				client := mockblobclient.NewMockClient(ctrl)
				resolver.EXPECT().Resolve(gomock.Any()).Return([]blobclient.Client{client}, nil)
				client.EXPECT().Addr().Return(testOrigin1).AnyTimes()
				return resolver
			},
			makeRequest: func(client blobclient.Client) error {
				return httputil.StatusError{Status: http.StatusAccepted}
			},
			backoff:     &testBackoff{maxCalls: 2},
			wantErr:     true,
			errContains: "backoff timed out",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			resolver := tt.setup(ctrl)
			err := blobclient.Poll(resolver, tt.backoff, core.DigestFixture(), tt.makeRequest)

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
