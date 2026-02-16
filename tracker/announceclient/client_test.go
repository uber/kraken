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
package announceclient_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	mockhashring "github.com/uber/kraken/mocks/lib/hashring"
	"github.com/uber/kraken/tracker/announceclient"
)

// =============================================================================
// Test Helpers
// =============================================================================

// stripHTTPPrefix removes http:// prefix from URL.
func stripHTTPPrefix(url string) string {
	return strings.TrimPrefix(url, "http://")
}

// testServer creates a test HTTP server and returns its address (without http://).
func testServer(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return stripHTTPPrefix(server.URL)
}

// =============================================================================
// Tests
// =============================================================================

// TestCheckReadiness verifies that CheckReadiness correctly handles
// various ring states and server responses.
func TestCheckReadiness(t *testing.T) {
	tests := []struct {
		name        string
		locations   []string
		serverReady bool
		wantErr     bool
		errContains string
	}{
		// --- Error Cases ---
		{
			name:        "server not ready",
			locations:   nil, // Will be set to test server address
			serverReady: false,
			wantErr:     true,
			errContains: "tracker not ready",
		},

		// --- Success Cases ---
		{
			name:        "server ready",
			locations:   nil, // Will be set to test server address
			serverReady: true,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctrl := gomock.NewController(t)

			locations := tt.locations
			if locations == nil {
				// Create a test server for cases that need one
				addr := testServer(t, func(w http.ResponseWriter, r *http.Request) {
					if tt.serverReady {
						w.WriteHeader(http.StatusOK)
					} else {
						w.WriteHeader(http.StatusServiceUnavailable)
					}
				})
				locations = []string{addr}
			}

			ring := mockhashring.NewMockPassiveRing(ctrl)
			ring.EXPECT().Locations(backend.ReadinessCheckDigest).Return(locations)

			client := announceclient.New(core.PeerContext{}, ring, nil)

			err := client.CheckReadiness()

			if tt.wantErr {
				require.Error(err)
				if tt.errContains != "" {
					require.Contains(err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(err)
		})
	}
}
