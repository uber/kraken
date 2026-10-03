// Copyright (c) 2016-2025 Uber Technologies, Inc.
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
package dockerregistry

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/utils/dockerutil"
)

// buildDriverWithVerification sets up a storage driver, backing transferer state,
// and returns a manifest tag path that will trigger manifest download + verify.
func buildDriverWithVerification(
	t *testing.T,
	decision SignatureVerificationDecision,
	retErr error,
	called *bool,
	enforceVerification bool,
) (*KrakenStorageDriver, string, string) {
	t.Helper()

	// Create CA store + transferer
	td, cleanup := newTestDriver()
	t.Cleanup(cleanup)

	// Prepare blobs and manifest in transferer
	config := core.NewBlobFixture()
	layer1 := core.NewBlobFixture()
	layer2 := core.NewBlobFixture()

	manifestDigest, manifestRaw := dockerutil.ManifestFixture(config.Digest, layer1.Digest, layer2.Digest)

	for _, blob := range []*core.BlobFixture{config, layer1, layer2} {
		require.NoError(t, td.transferer.Upload("unused", blob.Digest, store.NewBufferFileReader(blob.Content)))
	}
	require.NoError(t, td.transferer.Upload("unused", manifestDigest, store.NewBufferFileReader(manifestRaw)))

	repo := repoName
	tag := tagName
	require.NoError(t, td.transferer.PutTag(fmt.Sprintf("%s:%s", repo, tag), manifestDigest))

	// Custom verification function under test
	verif := func(vRepo string, vDigest core.Digest, blob store.FileReader) (SignatureVerificationDecision, error) {
		if called != nil {
			*called = true
		}
		// Basic sanity that verify receives expected repo/digest
		require.Equal(t, repo, vRepo)
		require.Equal(t, manifestDigest, vDigest)
		return decision, retErr
	}
	cfg := Config{EnforceSignatureVerification: enforceVerification}
	sd := NewReadWriteStorageDriver(cfg, td.cas, td.transferer, verif)

	// Path that triggers manifests.getDigest → verifySignature
	path := genManifestTagCurrentLinkPath(repo, tag, manifestDigest.Hex())
	return sd, path, ""
}

func TestVerification(t *testing.T) {
	tests := map[string]struct {
		decision            SignatureVerificationDecision
		retErr              error
		enforceVerification bool
		wantErr             string
	}{
		"allow without enforcement": {
			decision:            DecisionAllow,
			enforceVerification: false,
		},
		"skip without enforcement": {
			decision:            DecisionSkip,
			enforceVerification: false,
		},
		"deny without enforcement": {
			decision:            DecisionDeny,
			enforceVerification: false,
		},
		"error without enforcement": {
			retErr:              fmt.Errorf("test err"),
			enforceVerification: false,
		},
		"unknown without enforcement": {
			decision:            100,
			enforceVerification: false,
		},
		"allow with enforcement": {
			decision:            DecisionAllow,
			enforceVerification: true,
		},
		"skip with enforcement": {
			decision:            DecisionSkip,
			enforceVerification: true,
		},
		"deny with enforcement": {
			decision:            DecisionDeny,
			enforceVerification: true,
			wantErr:             "verify signature: denied sha256:",
		},
		"error with enforcement": {
			retErr:              fmt.Errorf("test err"),
			enforceVerification: true,
			wantErr:             "verify signature: test err",
		},
		"unknown with enforcement": {
			decision:            100,
			enforceVerification: true,
			wantErr:             "verify signature: unknown verification decision: 100",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var called bool
			sd, path, _ := buildDriverWithVerification(t, tt.decision, tt.retErr, &called, tt.enforceVerification)

			data, err := sd.GetContent(contextFixture(), path)
			require.True(t, called)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Greater(t, len(data), 0)
		})
	}
}
