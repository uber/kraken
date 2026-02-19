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
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/utils/dockerutil"
)

func TestVerificationCacheConfig_ApplyDefaults(t *testing.T) {
	c := VerificationCacheConfig{}
	c = c.applyDefaults()

	require.Equal(t, defaultVerificationCacheSize, c.Size,
		"default size must be %d", defaultVerificationCacheSize)
	require.Equal(t, defaultVerificationCacheTTL, c.TTL,
		"default TTL must be %v", defaultVerificationCacheTTL)
}

func TestVerificationCacheConfig_ApplyDefaults_PreservesExplicitValues(t *testing.T) {
	c := VerificationCacheConfig{Size: 500, TTL: 10 * time.Minute}
	c = c.applyDefaults()

	require.Equal(t, 500, c.Size)
	require.Equal(t, 10*time.Minute, c.TTL)
}

func TestVerificationCacheConfig_DefaultConstants(t *testing.T) {
	// Verify the hardcoded defaults match PR #441 values.
	require.Equal(t, 300, defaultVerificationCacheSize)
	require.Equal(t, 5*time.Minute, defaultVerificationCacheTTL)
}

func TestVerificationCacheConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  VerificationCacheConfig
		wantErr string
	}{
		{
			name:   "valid after defaults",
			config: VerificationCacheConfig{Size: 300, TTL: 5 * time.Minute},
		},
		{
			name:   "valid custom values",
			config: VerificationCacheConfig{Size: 1000, TTL: 30 * time.Minute},
		},
		{
			name:    "invalid zero size",
			config:  VerificationCacheConfig{Size: 0, TTL: 5 * time.Minute},
			wantErr: "verification_cache.size must be > 0",
		},
		{
			name:    "invalid negative size",
			config:  VerificationCacheConfig{Size: -1, TTL: 5 * time.Minute},
			wantErr: "verification_cache.size must be > 0",
		},
		{
			name:    "invalid zero TTL",
			config:  VerificationCacheConfig{Size: 300, TTL: 0},
			wantErr: "verification_cache.ttl must be a positive duration",
		},
		{
			name:    "invalid negative TTL",
			config:  VerificationCacheConfig{Size: 300, TTL: -time.Second},
			wantErr: "verification_cache.ttl must be a positive duration",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.wantErr)
			}
		})
	}
}

// buildDriverForCacheTest creates a storage driver with a controllable verification
// function and returns it along with a call counter.
func buildDriverForCacheTest(
	t *testing.T,
	cacheConfig VerificationCacheConfig,
	decision SignatureVerificationDecision,
	retErr error,
) (*KrakenStorageDriver, *int, string, core.Digest) {
	t.Helper()

	td, cleanup := newTestDriver()
	t.Cleanup(cleanup)

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

	callCount := 0
	verif := func(vRepo string, vDigest core.Digest, blob store.FileReader) (SignatureVerificationDecision, error) {
		callCount++
		return decision, retErr
	}

	driverConfig := Config{VerificationCache: cacheConfig}
	sd := NewReadWriteStorageDriver(driverConfig, td.cas, td.transferer, verif)

	path := genManifestTagCurrentLinkPath(repo, tag, manifestDigest.Hex())
	return sd, &callCount, path, manifestDigest
}

func TestVerificationCache_DefaultConfigBehavior(t *testing.T) {
	// When no config is provided (zero values), defaults are applied and
	// behavior is identical to PR #441: cache is active with size=300, TTL=5m.
	sd, callCount, path, manifestDigest := buildDriverForCacheTest(t, VerificationCacheConfig{}, DecisionAllow, nil)

	digestKey := manifestDigest.String()

	// Before any call the cache must be empty.
	require.False(t, sd.manifests.verifyCache.Has(digestKey),
		"digest must not be in cache before first call")

	// First call: verification runs. DecisionAllow emits no logs so it must NOT be cached.
	_, err := sd.GetContent(contextFixture(), path)
	require.NoError(t, err)
	require.Equal(t, 1, *callCount)
	require.False(t, sd.manifests.verifyCache.Has(digestKey),
		"DecisionAllow must not be cached (nothing to deduplicate)")

	// Second call: verification still runs (always executed) and still not cached.
	_, err = sd.GetContent(contextFixture(), path)
	require.NoError(t, err)
	require.Equal(t, 2, *callCount, "verification function should always be called")
	require.False(t, sd.manifests.verifyCache.Has(digestKey),
		"DecisionAllow must remain uncached across calls")
}

func TestVerificationCache_CustomSizeOverride(t *testing.T) {
	cacheConfig := VerificationCacheConfig{Size: 500, TTL: 10 * time.Minute}
	sd, callCount, path, manifestDigest := buildDriverForCacheTest(t, cacheConfig, DecisionAllow, nil)

	_, err := sd.GetContent(contextFixture(), path)
	require.NoError(t, err)
	require.Equal(t, 1, *callCount)
	require.False(t, sd.manifests.verifyCache.Has(manifestDigest.String()),
		"DecisionAllow must not be cached even with custom size")

	_, err = sd.GetContent(contextFixture(), path)
	require.NoError(t, err)
	require.Equal(t, 2, *callCount, "verification function should always be called with custom size")
}

func TestVerificationCache_CustomTTLOverride(t *testing.T) {
	cacheConfig := VerificationCacheConfig{Size: 300, TTL: 1 * time.Minute}
	sd, callCount, path, manifestDigest := buildDriverForCacheTest(t, cacheConfig, DecisionSkip, nil)

	digestKey := manifestDigest.String()

	// First call: DecisionSkip emits a log and IS cached.
	_, err := sd.GetContent(contextFixture(), path)
	require.NoError(t, err)
	require.Equal(t, 1, *callCount)
	require.True(t, sd.manifests.verifyCache.Has(digestKey),
		"DecisionSkip must be cached (log dedup)")

	// Second call: still executed, but the skip-log is suppressed (cached).
	_, err = sd.GetContent(contextFixture(), path)
	require.NoError(t, err)
	require.Equal(t, 2, *callCount, "verification function should always be called with custom TTL")
	require.True(t, sd.manifests.verifyCache.Has(digestKey),
		"DecisionSkip must remain cached across calls")
}

func TestVerificationCache_DenyNotCached(t *testing.T) {
	// Denied verifications should not be cached, so the deny warning is
	// logged every time.
	sd, callCount, path, manifestDigest := buildDriverForCacheTest(t, VerificationCacheConfig{}, DecisionDeny, nil)

	digestKey := manifestDigest.String()

	_, err := sd.GetContent(contextFixture(), path)
	require.NoError(t, err)
	require.Equal(t, 1, *callCount)
	require.False(t, sd.manifests.verifyCache.Has(digestKey),
		"DecisionDeny must not be cached")

	_, err = sd.GetContent(contextFixture(), path)
	require.NoError(t, err)
	require.Equal(t, 2, *callCount)
	require.False(t, sd.manifests.verifyCache.Has(digestKey),
		"DecisionDeny must remain uncached across calls")
}

func TestVerificationCache_ErrorNotCached(t *testing.T) {
	// Errors should not be cached.
	sd, callCount, path, manifestDigest := buildDriverForCacheTest(t, VerificationCacheConfig{}, DecisionAllow, fmt.Errorf("boom"))

	digestKey := manifestDigest.String()

	_, err := sd.GetContent(contextFixture(), path)
	require.NoError(t, err) // verify errors are currently ignored by getDigest
	require.Equal(t, 1, *callCount)
	require.False(t, sd.manifests.verifyCache.Has(digestKey),
		"errors must not be cached")

	_, err = sd.GetContent(contextFixture(), path)
	require.NoError(t, err)
	require.Equal(t, 2, *callCount)
	require.False(t, sd.manifests.verifyCache.Has(digestKey),
		"errors must remain uncached across calls")
}
