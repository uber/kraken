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
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/utils/dockerutil"
)

// buildDriverWithVerification sets up a storage driver, backing transferer state,
// and returns a manifest tag path that will trigger manifest download + verify.
func buildDriverWithVerification(t *testing.T, decision SignatureVerificationDecision, retErr error, called *bool) (*KrakenStorageDriver, string, string) {
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
	sd := NewReadWriteStorageDriver(Config{}, td.cas, td.transferer, verif, tally.NoopScope)

	// Path that triggers manifests.getDigest → verify
	path := genManifestTagCurrentLinkPath(repo, tag, manifestDigest.Hex())
	return sd, path, ""
}

func TestVerification_Allow(t *testing.T) {
	var called bool
	sd, path, _ := buildDriverWithVerification(t, DecisionAllow, nil, &called)

	data, err := sd.GetContent(contextFixture(), path)
	require.NoError(t, err)
	// Should still return a digest link (sha256:<hex>)
	require.Greater(t, len(data), 0)
	require.True(t, called, "verification should be called")
}

func TestVerification_Deny(t *testing.T) {
	var called bool
	sd, path, _ := buildDriverWithVerification(t, DecisionDeny, nil, &called)

	data, err := sd.GetContent(contextFixture(), path)
	require.NoError(t, err)
	// Deny does not block returning the manifest link today (verify is advisory)
	require.Greater(t, len(data), 0)
	require.True(t, called, "verification should be called")
}

func TestVerification_Skip(t *testing.T) {
	var called bool
	sd, path, _ := buildDriverWithVerification(t, DecisionSkip, nil, &called)

	data, err := sd.GetContent(contextFixture(), path)
	require.NoError(t, err)
	require.Greater(t, len(data), 0)
	require.True(t, called, "verification should be called")
}

func TestVerification_Error(t *testing.T) {
	var called bool
	sd, path, _ := buildDriverWithVerification(t, DecisionAllow /*unused*/, fmt.Errorf("boom"), &called)

	data, err := sd.GetContent(contextFixture(), path)
	// Even on error, current behavior is to ignore verify error and proceed
	require.NoError(t, err)
	require.Greater(t, len(data), 0)
	require.True(t, called, "verification should be called")
}
