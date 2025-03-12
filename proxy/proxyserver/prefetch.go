package proxyserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/log"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
)

// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// PrefetchHandler defines the handler of preheat.
type PrefetchHandler struct {
	clusterClient blobclient.ClusterClient
	tagClient     tagclient.Client
	threshold     int64

	metrics tally.Scope
}

// prefetchBody defines the body of preheat.
type prefetchBody struct {
	// Tag is the tag of the image. The format api expect is: address/namespace/tag.
	// Example: 127.0.0.1:5055/uber-usi/abacus-go:bkt1-produ-1700103798-0154d
	Tag     string `json:"tag"`
	TraceId string `json:"trace_id"`
}

const StatusSuccess = "success"
const StatusFailure = "failure"

type prefetchResponse struct {
	Tag        string `json:"tag"`
	Prefetched bool   `json:"prefetched"`
	Status     string `json:"status"`
	Message    string `json:"message"`
	TraceId    string `json:"trace_id"`
}

func newPrefetchResponse(tag string, msg string, traceId string) *prefetchResponse {
	return &prefetchResponse{
		Tag:        tag,
		Prefetched: true,
		Status:     StatusSuccess,
		Message:    msg,
		TraceId:    traceId,
	}
}

func writePrefetchResponse(w http.ResponseWriter, status int, tag string, msg string, traceId string) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(newPrefetchResponse(tag, msg, traceId))
}

type prefetchError struct {
	Error      string `json:"error"`
	Prefetched bool   `json:"prefetched"`
	Status     string `json:"status"`
	Message    string `json:"message"`
	TraceId    string `json:"trace_id,omitempty"`
}

func newPrefetchError(status int, msg string, traceId string) *prefetchError {
	return &prefetchError{
		Error:      http.StatusText(status),
		Prefetched: false,
		Status:     StatusFailure,
		Message:    msg,
		TraceId:    traceId,
	}
}

func writeBadRequestError(w http.ResponseWriter, msg string, traceId string) {
	status := http.StatusBadRequest
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(newPrefetchError(status, msg, traceId))
}

func writeInternalError(w http.ResponseWriter, msg string, traceId string) {
	status := http.StatusInternalServerError
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(newPrefetchError(status, msg, traceId))
}

// NewPrefetchHandler creates a new preheat handler.
func NewPrefetchHandler(client blobclient.ClusterClient, tagClient tagclient.Client, threshold int64, metrics tally.Scope) *PrefetchHandler {
	return &PrefetchHandler{client, tagClient, threshold, metrics}
}

// Handle processes the prefetch request.
func (ph *PrefetchHandler) Handle(w http.ResponseWriter, r *http.Request) {
	var reqBody prefetchBody
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		writeBadRequestError(w, fmt.Sprintf("failed to decode request body: %s", err), "")
		log.With("error", err).Error("Failed to decode request body")
		return
	}
	logger := log.With("trace_id", reqBody.TraceId)

	namespace, tag, err := parseTag(reqBody.Tag)
	if err != nil {
		writeBadRequestError(w, fmt.Sprintf("tag: %s, invalid tag format: %s", reqBody.Tag, err), reqBody.TraceId)
		return
	}

	tagRequest := fmt.Sprintf("%s%%2F%s", namespace, tag)
	digest, err := ph.tagClient.Get(tagRequest)
	if err != nil {
		writeInternalError(w, fmt.Sprintf("tag request: %s, failed to get tag: %s", tagRequest, err), reqBody.TraceId)
		return
	}

	stat, err := ph.clusterClient.Stat(namespace, digest)
	if err != nil {
		writeInternalError(w, fmt.Sprintf("digest %s, failed to get meta info: %s", digest, err), reqBody.TraceId)
		return
	}
	logger.Infof("Namespace: %s, Tag: %s, Size: %d", namespace, tag, stat.Size)

	buf := &bytes.Buffer{}
	if err := ph.clusterClient.DownloadBlob(namespace, digest, buf); err != nil {
		writeInternalError(w, fmt.Sprintf("error downloading manifest blob: %s", err), reqBody.TraceId)
		return
	}

	// Process manifest (ManifestList or single Manifest)
	size, digests, err := ph.processManifest(logger, namespace, buf.Bytes())
	if err != nil {
		writeInternalError(w, fmt.Sprintf("failed to process manifest: %s", err), reqBody.TraceId)
		return
	}
	size += stat.Size

	metrics := ph.metrics.SubScope("prefetch")
	if size < ph.threshold {
		metrics.Counter("below_threshold").Inc(1)
		logger.With("size", size, "tag", reqBody.Tag, "threshold", ph.threshold).Info("Size is too large, skipping prefetch")
		writePrefetchResponse(w, http.StatusUnprocessableEntity, reqBody.Tag, fmt.Sprintf("prefetching not initiated as imagesize < %d", ph.threshold), reqBody.TraceId)
		return
	}

	metrics.Counter("initiated").Inc(1)
	writePrefetchResponse(w, http.StatusOK, reqBody.Tag, "prefetching initiated successfully", reqBody.TraceId)

	go func() {
		var wg sync.WaitGroup
		var mu sync.Mutex
		var errList []error

		for _, digest := range digests {
			wg.Add(1)
			go func(digest core.Digest) {
				defer wg.Done()
				if err := ph.clusterClient.DownloadBlob(namespace, digest, ioutil.Discard); err != nil {
					if serr, ok := err.(httputil.StatusError); ok && serr.Status == http.StatusAccepted {
						return
					}
					mu.Lock()
					errList = append(errList, fmt.Errorf("digest %s, namespace %s, error downloading blob: %w", digest, namespace, err))
					mu.Unlock()
				}
			}(digest)
		}

		wg.Wait()

		// Log errors if any
		if len(errList) > 0 {
			metrics.Counter("failed").Inc(1)
			for _, err := range errList {
				logger.With("error", err).Error("Error downloading blob")
			}
		}
	}()
}

// parseTag extracts namespace and tag from a given tag string.
func parseTag(tag string) (namespace, name string, err error) {
	parts := strings.Split(tag, "/")
	if len(parts) < 3 {
		return "", "", fmt.Errorf("invalid tag format: %s", tag)
	}
	return parts[1], parts[2], nil
}

// processManifest handles both ManifestLists and single Manifests.
func (ph *PrefetchHandler) processManifest(logger *zap.SugaredLogger, namespace string, manifestBytes []byte) (int64, []core.Digest, error) {
	reader := bytes.NewReader(manifestBytes)

	var manifestList manifestlist.ManifestList
	if err := json.NewDecoder(reader).Decode(&manifestList); err == nil && len(manifestList.Manifests) > 0 {
		logger.With("namespace", namespace).Info("Processing manifest list")
		return ph.processManifestList(logger, namespace, manifestList)
	}

	reader = bytes.NewReader(manifestBytes) // Reset reader for second attempt
	var manifest schema2.Manifest
	if err := json.NewDecoder(reader).Decode(&manifest); err != nil {
		logger.With("namespace", namespace).Errorf("Failed to parse single manifest: %v", err)
		return 0, nil, fmt.Errorf("invalid single manifest: %w", err)
	}

	return ph.processLayers(namespace, manifest.Layers)
}

func (ph *PrefetchHandler) processManifestList(logger *zap.SugaredLogger, namespace string, manifestList manifestlist.ManifestList) (int64, []core.Digest, error) {
	digestsResult := make([]core.Digest, 0)
	sizeResult := int64(0)
	buf := &bytes.Buffer{}
	for _, manifestDescriptor := range manifestList.Manifests {
		manifestDigestHex := manifestDescriptor.Digest.Hex()
		digest, err := core.NewSHA256DigestFromHex(manifestDigestHex)
		if err != nil {
			return 0, nil, fmt.Errorf("digest %s, failed to parse manifest digest: %s", manifestDigestHex, err)
		}

		stat, err := ph.clusterClient.Stat(namespace, digest)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to get meta info: %w", err)
		}

		sizeResult += stat.Size
		if err := ph.clusterClient.DownloadBlob(namespace, digest, buf); err != nil {
			logger.Errorf("Failed to download manifest blob: %s", err)
			continue
		}

		var manifest schema2.Manifest
		if err := json.NewDecoder(buf).Decode(&manifest); err != nil {
			return 0, nil, fmt.Errorf("failed to parse manifest: %s", err)
		}

		size, digests, err := ph.processLayers(namespace, manifest.Layers)
		if err != nil {
			return 0, nil, err
		}
		digestsResult = append(digestsResult, digests...)
		sizeResult += size
		buf.Reset()
	}
	return sizeResult, digestsResult, nil
}

func (ph *PrefetchHandler) processLayers(namespace string, layers []distribution.Descriptor) (int64, []core.Digest, error) {
	digests := make([]core.Digest, 0, len(layers))
	var totalSize int64

	for _, layer := range layers {
		digest, err := core.NewSHA256DigestFromHex(layer.Digest.Hex())
		if err != nil {
			return 0, nil, fmt.Errorf("invalid layer digest: %w", err)
		}

		stat, err := ph.clusterClient.Stat(namespace, digest)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to get metadata for layer %s: %w", digest, err)
		}
		totalSize += stat.Size
		digests = append(digests, digest)
	}
	return totalSize, digests, nil
}
