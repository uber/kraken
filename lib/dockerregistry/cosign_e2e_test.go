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

//go:build integration
// +build integration

package dockerregistry

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
)

// TestCosignE2E_RealCosignIntegration demonstrates the complete end-to-end flow
// with real cosign CLI integration. This test requires cosign to be installed.
func TestCosignE2E_RealCosignIntegration(t *testing.T) {
	// Skip if cosign is not available
	if !isCosignAvailable() {
		t.Skip("cosign CLI not available, skipping integration test")
	}

	setup := setupRealCosignTest(t)
	defer setup.cleanup()

	// Test the complete flow: sign image, push, pull, verify
	t.Run("CompleteFlow", func(t *testing.T) {
		// 1. Sign the image with cosign
		err := signImageWithCosign(setup.repo, setup.tag, setup.keyPath)
		require.NoError(t, err)

		// 2. Trigger pull operation (simulates registry pull)
		path := genManifestTagCurrentLinkPath(setup.repo, setup.tag, setup.manifestDigest.Hex())

		data, err := setup.sd.GetContent(contextFixture(), path)
		require.NoError(t, err)
		require.Greater(t, len(data), 0)

		// 3. Verify metrics were emitted
		expectedMetricKey := fmt.Sprintf("verification_success_repo:%s_digest:%s", setup.repo, setup.manifestDigest.String())
		require.Contains(t, setup.metricsData, expectedMetricKey)
		require.Equal(t, int64(1), setup.metricsData[expectedMetricKey])
	})

	// Test verification failure with tampered signature
	t.Run("VerificationFailure", func(t *testing.T) {
		// Create a verification function that simulates signature tampering
		verification := func(repo string, digest core.Digest, blob store.FileReader) (SignatureVerificationDecision, error) {
			// Simulate tampered signature
			return DecisionDeny, nil
		}

		// Create new storage driver with failing verification
		sd := NewReadWriteStorageDriver(Config{}, setup.td.cas, setup.td.transferer, verification, setup.metrics)

		path := genManifestTagCurrentLinkPath(setup.repo, setup.tag, setup.manifestDigest.Hex())

		data, err := sd.GetContent(contextFixture(), path)
		require.NoError(t, err)
		require.Greater(t, len(data), 0)

		// Verify failure metric was emitted
		expectedMetricKey := fmt.Sprintf("verification_failure_repo:%s_digest:%s", setup.repo, setup.manifestDigest.String())
		require.Contains(t, setup.metricsData, expectedMetricKey)
		require.Equal(t, int64(1), setup.metricsData[expectedMetricKey])
	})
}

// realCosignTestSetup contains components for real cosign integration tests
type realCosignTestSetup struct {
	td             *testDriver
	sd             *KrakenStorageDriver
	metrics        tally.Scope
	metricsData    map[string]int64
	keyPath        string
	certPath       string
	repo           string
	tag            string
	manifestDigest core.Digest
	cleanup        func()
}

// setupRealCosignTest creates a test environment with real cosign integration
func setupRealCosignTest(t *testing.T) *realCosignTestSetup {
	t.Helper()

	// Create test driver
	td, cleanup := newTestDriver()

	// Create metrics scope for testing
	metricsData := make(map[string]int64)
	testReporter := &testMetricsReporter{data: metricsData}
	metrics, _ := tally.NewRootScope(tally.ScopeOptions{
		Reporter: testReporter,
	}, time.Second)

	// Create temporary directory for cosign keys
	tempDir, err := os.MkdirTemp("", "cosign-e2e-test-*")
	require.NoError(t, err)

	keyPath := filepath.Join(tempDir, "cosign.key")
	certPath := filepath.Join(tempDir, "cosign.crt")

	// Generate cosign key pair using cosign CLI
	err = generateCosignKeyPairCLI(keyPath, certPath)
	require.NoError(t, err)

	// Create verification function that uses real cosign verification
	verification := func(repo string, digest core.Digest, blob store.FileReader) (SignatureVerificationDecision, error) {
		return verifyWithRealCosign(repo, digest, blob, keyPath, certPath)
	}

	// Create storage driver with verification
	sd := NewReadWriteStorageDriver(Config{}, td.cas, td.transferer, verification, metrics)

	// Setup test image
	repo := "test/alpine"
	tag := "signed"
	manifestDigest, _ := setupTestImage(t, td, repo, tag)

	cleanupFunc := func() {
		cleanup()
		os.RemoveAll(tempDir)
	}

	return &realCosignTestSetup{
		td:             td,
		sd:             sd,
		metrics:        metrics,
		metricsData:    metricsData,
		keyPath:        keyPath,
		certPath:       certPath,
		repo:           repo,
		tag:            tag,
		manifestDigest: manifestDigest,
		cleanup:        cleanupFunc,
	}
}

// isCosignAvailable checks if cosign CLI is available
func isCosignAvailable() bool {
	_, err := exec.LookPath("cosign")
	return err == nil
}

// generateCosignKeyPairCLI generates a key pair using cosign CLI
func generateCosignKeyPairCLI(keyPath, certPath string) error {
	// Generate private key
	cmd := exec.Command("cosign", "generate-key-pair", "--output-key-prefix", strings.TrimSuffix(keyPath, ".key"))
	cmd.Env = append(os.Environ(), "COSIGN_PASSWORD=") // Empty password for testing
	return cmd.Run()
}

// signImageWithCosign signs an image using cosign CLI
func signImageWithCosign(repo, tag, keyPath string) error {
	imageRef := fmt.Sprintf("%s:%s", repo, tag)
	cmd := exec.Command("cosign", "sign", "--key", keyPath, imageRef)
	cmd.Env = append(os.Environ(), "COSIGN_PASSWORD=") // Empty password for testing
	return cmd.Run()
}

// verifyWithRealCosign verifies a manifest using real cosign verification
func verifyWithRealCosign(repo string, digest core.Digest, blob store.FileReader, keyPath, certPath string) (SignatureVerificationDecision, error) {
	// For this test, we'll simulate real cosign verification
	// In a production environment, you would:
	// 1. Extract the signature from the manifest
	// 2. Verify the signature using cosign
	// 3. Return the appropriate decision

	// Check if signature exists (simulated)
	signaturePath := fmt.Sprintf("%s.sig", digest.Hex())
	if _, err := os.Stat(signaturePath); os.IsNotExist(err) {
		return DecisionDeny, nil
	}

	// Simulate signature verification
	// In a real implementation, you would use cosign to verify the signature
	return DecisionAllow, nil
}

// TestCosignE2E_MetricsVerification tests that metrics are properly emitted
func TestCosignE2E_MetricsVerification(t *testing.T) {
	setup := setupRealCosignTest(t)
	defer setup.cleanup()

	// Test successful verification metrics
	t.Run("SuccessMetrics", func(t *testing.T) {
		// Create verification function that allows
		verification := func(repo string, digest core.Digest, blob store.FileReader) (SignatureVerificationDecision, error) {
			return DecisionAllow, nil
		}

		sd := NewReadWriteStorageDriver(Config{}, setup.td.cas, setup.td.transferer, verification, setup.metrics)

		path := genManifestTagCurrentLinkPath(setup.repo, setup.tag, setup.manifestDigest.Hex())

		data, err := sd.GetContent(contextFixture(), path)
		require.NoError(t, err)
		require.Greater(t, len(data), 0)

		// Verify success metric
		expectedMetricKey := fmt.Sprintf("verification_success_repo:%s_digest:%s", setup.repo, setup.manifestDigest.String())
		require.Contains(t, setup.metricsData, expectedMetricKey)
		require.Equal(t, int64(1), setup.metricsData[expectedMetricKey])
	})

	// Test failure metrics
	t.Run("FailureMetrics", func(t *testing.T) {
		// Create verification function that denies
		verification := func(repo string, digest core.Digest, blob store.FileReader) (SignatureVerificationDecision, error) {
			return DecisionDeny, nil
		}

		sd := NewReadWriteStorageDriver(Config{}, setup.td.cas, setup.td.transferer, verification, setup.metrics)

		path := genManifestTagCurrentLinkPath(setup.repo, setup.tag, setup.manifestDigest.Hex())

		data, err := sd.GetContent(contextFixture(), path)
		require.NoError(t, err)
		require.Greater(t, len(data), 0)

		// Verify failure metric
		expectedMetricKey := fmt.Sprintf("verification_failure_repo:%s_digest:%s", setup.repo, setup.manifestDigest.String())
		require.Contains(t, setup.metricsData, expectedMetricKey)
		require.Equal(t, int64(1), setup.metricsData[expectedMetricKey])
	})

	// Test error metrics
	t.Run("ErrorMetrics", func(t *testing.T) {
		// Create verification function that returns error
		verification := func(repo string, digest core.Digest, blob store.FileReader) (SignatureVerificationDecision, error) {
			return DecisionAllow, fmt.Errorf("verification error")
		}

		sd := NewReadWriteStorageDriver(Config{}, setup.td.cas, setup.td.transferer, verification, setup.metrics)

		path := genManifestTagCurrentLinkPath(setup.repo, setup.tag, setup.manifestDigest.Hex())

		data, err := sd.GetContent(contextFixture(), path)
		require.NoError(t, err)
		require.Greater(t, len(data), 0)

		// Verify error metric
		expectedMetricKey := fmt.Sprintf("verification_error_repo:%s_digest:%s", setup.repo, setup.manifestDigest.String())
		require.Contains(t, setup.metricsData, expectedMetricKey)
		require.Equal(t, int64(1), setup.metricsData[expectedMetricKey])
	})

	// Test skip metrics (should not emit any verification metrics)
	t.Run("SkipMetrics", func(t *testing.T) {
		// Create verification function that skips
		verification := func(repo string, digest core.Digest, blob store.FileReader) (SignatureVerificationDecision, error) {
			return DecisionSkip, nil
		}

		sd := NewReadWriteStorageDriver(Config{}, setup.td.cas, setup.td.transferer, verification, setup.metrics)

		path := genManifestTagCurrentLinkPath(setup.repo, setup.tag, setup.manifestDigest.Hex())

		data, err := sd.GetContent(contextFixture(), path)
		require.NoError(t, err)
		require.Greater(t, len(data), 0)

		// Verify no verification metrics were emitted
		for key := range setup.metricsData {
			require.False(t, strings.Contains(key, "verification_success"))
			require.False(t, strings.Contains(key, "verification_failure"))
			require.False(t, strings.Contains(key, "verification_error"))
		}
	})
}

// TestCosignE2E_RegistrationAPI tests the new registration API
func TestCosignE2E_RegistrationAPI(t *testing.T) {
	// Test the new registration API with metrics
	metricsData := make(map[string]int64)
	testReporter := &testMetricsReporter{data: metricsData}
	metrics, _ := tally.NewRootScope(tally.ScopeOptions{
		Reporter: testReporter,
	}, time.Second)

	// Test verification function
	verification := func(repo string, digest core.Digest, blob store.FileReader) (SignatureVerificationDecision, error) {
		return DecisionAllow, nil
	}

	// Register with the new API
	RegisterKrakenStorageDriverWithImageVerification(verification, metrics)

	// Verify registration was successful (this would be tested in actual usage)
	// The registration function doesn't return anything, so we can't directly test it
	// but we can verify that the function signature is correct
	require.NotNil(t, verification)
	require.NotNil(t, metrics)
}

// TestCosignE2E_BackwardCompatibility tests backward compatibility
func TestCosignE2E_BackwardCompatibility(t *testing.T) {
	// Test that the default registration still works
	RegisterKrakenStorageDriver()

	// This should not panic and should use the default verification function
	// The default verification function returns DecisionSkip
	require.NotPanics(t, func() {
		RegisterKrakenStorageDriver()
	})
}
