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
package dockerregistry

import (
	"fmt"
	"strings"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/lib/store"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/utils/log"
)

const (
	signatureVerificationSuccessCounter = "signature_verification_success"
	signatureVerificationFailureCounter = "signature_verification_failure"
	signatureVerificationErrorCounter   = "signature_verification_error"
	signatureVerificationDuration       = "signature_verification_duration"
)

type SignatureVerificationDecision int

const (
	DecisionSkip SignatureVerificationDecision = iota
	DecisionDeny
	DecisionAllow
)

type manifests struct {
	transferer   transfer.ImageTransferer
	verification func(repo string, digest core.Digest, blob store.FileReader) (SignatureVerificationDecision, error)
	metrics      tally.Scope
}

func newManifests(
	transferer transfer.ImageTransferer,
	verification func(repo string, digest core.Digest, blob store.FileReader) (SignatureVerificationDecision, error),
	metrics tally.Scope,
) *manifests {
	return &manifests{transferer, verification, metrics}
}

// getDigest resolves and downloads a manifest blob (by tag or digest) and
// returns its digest as bytes.
//
// Behavior
//  1. Extracts the repository from the provided registry path.
//  2. If subtype is tags, resolves the tag to a digest using the transferer;
//     if subtype is revisions, parses the digest directly from the path.
//  3. Downloads the manifest blob via the transferer using (repo, digest).
//  4. Opportunistically invokes verify to run signature/image checks and
//     record metrics/logs. Verification result is not enforced here.
//  5. Returns the digest in ASCII string form as a byte slice.
//
// Notes
//   - This is the single place where a manifest is actually fetched via the
//     transferer (torrent/origin), since it has the namespace (repo) context.
//   - Callers typically invoke getDigest first to ensure the blob is local,
//     then call Stat/Reader which assume the blob is already on disk.
func (t *manifests) getDigest(path string, subtype PathSubType) ([]byte, error) {
	repo, err := GetRepo(path)
	if err != nil {
		return nil, fmt.Errorf("get repo: %s", err)
	}

	var digest core.Digest
	switch subtype {
	case _tags:
		tag, _, err := GetManifestTag(path)
		if err != nil {
			return nil, fmt.Errorf("get manifest tag: %s", err)
		}
		digest, err = t.transferer.GetTag(fmt.Sprintf("%s:%s", repo, tag))
		if err != nil {
			return nil, fmt.Errorf("transferer get tag: %w", err)
		}
	case _revisions:
		var err error
		digest, err = GetManifestDigest(path)
		if err != nil {
			return nil, fmt.Errorf("get manifest digest: %s", err)
		}
	default:
		return nil, &InvalidRequestError{path}
	}

	blob, err := t.transferer.Download(repo, digest)
	if err != nil {
		return nil, fmt.Errorf("transferer download: %w", err)
	}
	defer blob.Close()

	_, _ = t.verify(path, repo, digest, blob)
	return []byte(digest.String()), nil
}

// verify runs signature/image verification for a downloaded manifest blob and
// records metrics/logs around the decision and duration.
//
// Returns
//   - (true, nil)  when verification is allowed or intentionally skipped.
//   - (false, nil) when verification explicitly denies.
//   - (false, err) on verification errors or unknown decisions.
//
// Metrics
//   - signature_verification_duration (timer): end-to-end verification latency.
//   - signature_verification_success  (counter): allow decisions.
//   - signature_verification_failure  (counter): deny decisions.
//   - signature_verification_error    (counter): errors/unknown decisions.
//
// Logging
//   - Error on verification error (includes repo/digest).
//   - Warn  on deny (includes original path).
//   - Debug on skip.
func (t *manifests) verify(path string, repo string, digest core.Digest, blob store.FileReader) (bool, error) {
	stopwatch := t.metrics.Timer(signatureVerificationDuration).Start()
	defer stopwatch.Stop()
	decision, err := t.verification(repo, digest, blob)
	if err != nil {
		t.metrics.Counter(signatureVerificationErrorCounter).Inc(1)
		log.With("repo", repo, "digest", digest).Errorf("Error while performing image validation %s", err)
		return false, err
	}
	switch decision {
	case DecisionAllow:
		t.metrics.Counter(signatureVerificationSuccessCounter).Inc(1)
		return true, nil
	case DecisionDeny:
		t.metrics.Counter(signatureVerificationFailureCounter).Inc(1)
		log.With("repo", repo, "digest", digest).Warnf("Verification failed %s", path)
		return false, nil
	case DecisionSkip:
		log.With("repo", repo, "digest", digest).Debugf("Verification skipped for %s", path)
		return true, nil
	default:
		t.metrics.Counter(signatureVerificationErrorCounter).Inc(1)
		return false, fmt.Errorf("unknown verification decision: %d", decision)
	}
}

func (t *manifests) putContent(path string, subtype PathSubType) error {
	switch subtype {
	case _tags:
		repo, err := GetRepo(path)
		if err != nil {
			return fmt.Errorf("get repo: %s", err)
		}
		tag, isCurrent, err := GetManifestTag(path)
		if err != nil {
			return fmt.Errorf("get manifest tag: %s", err)
		}
		if isCurrent {
			return nil
		}
		digest, err := GetManifestDigest(path)
		if err != nil {
			return fmt.Errorf("get manifest digest: %s", err)
		}
		if err := t.transferer.PutTag(fmt.Sprintf("%s:%s", repo, tag), digest); err != nil {
			return fmt.Errorf("post tag: %w", err)
		}
		return nil
	}
	// Intentional no-op.
	return nil
}

func (t *manifests) stat(path string) (storagedriver.FileInfo, error) {
	repo, err := GetRepo(path)
	if err != nil {
		return nil, fmt.Errorf("get repo: %s", err)
	}
	tag, _, err := GetManifestTag(path)
	if err != nil {
		return nil, fmt.Errorf("get manifest tag: %s", err)
	}
	if _, err := t.transferer.GetTag(fmt.Sprintf("%s:%s", repo, tag)); err != nil {
		return nil, fmt.Errorf("get tag: %w", err)
	}
	return storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Path:    path,
			Size:    64,
			ModTime: time.Now(),
			IsDir:   false,
		},
	}, nil
}

func (t *manifests) list(path string) ([]string, error) {
	prefix := path[len(_repositoryRoot):]
	tags, err := t.transferer.ListTags(prefix)
	if err != nil {
		return nil, err
	}
	for i, tag := range tags {
		// Strip repo prefix.
		parts := strings.Split(tags[i], ":")
		if len(parts) != 2 {
			log.With("tag", tag).Warn("Repo list skipping tag, expected repo:tag format")
			continue
		}
		tags[i] = parts[1]
	}
	return tags, nil
}
