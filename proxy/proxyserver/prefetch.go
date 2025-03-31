package proxyserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

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
)

// Constants for prefetch status.
const (
	StatusSuccess = "success"
	StatusFailure = "failure"
)

// PrefetchHandler handles prefetch requests.
type PrefetchHandler struct {
	clusterClient blobclient.ClusterClient
	tagClient     tagclient.Client
	metrics       tally.Scope
}

// countWriter is an io.Writer that wraps another writer and counts the number of bytes written.
type countWriter struct {
	writer io.Writer
	count  int64
}

func (cw *countWriter) Write(p []byte) (int, error) {
	n, err := cw.writer.Write(p)
	cw.count += int64(n)
	return n, err
}

// Request and response payloads.
type prefetchBody struct {
	Tag     string `json:"tag"`
	TraceId string `json:"trace_id"`
}

type prefetchResponse struct {
	Tag        string `json:"tag"`
	Prefetched bool   `json:"prefetched"`
	Status     string `json:"status"`
	Message    string `json:"message"`
	TraceId    string `json:"trace_id"`
}

type prefetchError struct {
	Error      string `json:"error"`
	Prefetched bool   `json:"prefetched"`
	Status     string `json:"status"`
	Message    string `json:"message"`
	TraceId    string `json:"trace_id,omitempty"`
}

// NewPrefetchHandler constructs a new PrefetchHandler.
func NewPrefetchHandler(client blobclient.ClusterClient, tagClient tagclient.Client, metrics tally.Scope) *PrefetchHandler {
	return &PrefetchHandler{
		clusterClient: client,
		tagClient:     tagClient,
		metrics:       metrics.SubScope("prefetch"),
	}
}

// newPrefetchResponse constructs a successful response.
func newPrefetchResponse(tag, msg, traceId string) *prefetchResponse {
	return &prefetchResponse{
		Tag:        tag,
		Prefetched: true,
		Status:     StatusSuccess,
		Message:    msg,
		TraceId:    traceId,
	}
}

// newPrefetchError constructs an error response.
func newPrefetchError(status int, msg, traceId string) *prefetchError {
	return &prefetchError{
		Error:      http.StatusText(status),
		Prefetched: false,
		Status:     StatusFailure,
		Message:    msg,
		TraceId:    traceId,
	}
}

// writeJSON writes the JSON payload with the given HTTP status.
func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(payload)
}

func writeBadRequestError(w http.ResponseWriter, msg, traceId string) {
	writeJSON(w, http.StatusBadRequest, newPrefetchError(http.StatusBadRequest, msg, traceId))
}

func writeInternalError(w http.ResponseWriter, msg, traceId string) {
	writeJSON(w, http.StatusInternalServerError, newPrefetchError(http.StatusInternalServerError, msg, traceId))
}

func writePrefetchResponse(w http.ResponseWriter, tag, msg, traceId string) {
	writeJSON(w, http.StatusOK, newPrefetchResponse(tag, msg, traceId))
}

// Handle processes the prefetch request.
func (ph *PrefetchHandler) Handle(w http.ResponseWriter, r *http.Request) {
	ph.metrics.Counter("requests").Inc(1)
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
	logger.Infof("Namespace: %s, Tag: %s", namespace, tag)

	buf := &bytes.Buffer{}
	if err := ph.clusterClient.DownloadBlob(namespace, digest, buf); err != nil {
		writeInternalError(w, fmt.Sprintf("error downloading manifest blob: %s", err), reqBody.TraceId)
		return
	}

	// Process manifest (ManifestList or single Manifest)
	digests, err := ph.processManifest(logger, namespace, buf.Bytes())
	if err != nil {
		writeInternalError(w, fmt.Sprintf("failed to process manifest: %s", err), reqBody.TraceId)
		return
	}

	ph.metrics.SubScope("prefetch").Counter("initiated").Inc(1)
	writePrefetchResponse(w, reqBody.Tag, "prefetching initiated successfully", reqBody.TraceId)

	// Prefetch blobs asynchronously.
	go ph.prefetchBlobs(logger, namespace, digests)
}

// prefetchBlobs downloads blobs in parallel.
func (ph *PrefetchHandler) prefetchBlobs(logger *zap.SugaredLogger, namespace string, digests []core.Digest) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errList []error
	metrics := ph.metrics.SubScope("prefetch")

	for _, d := range digests {
		wg.Add(1)
		go func(digest core.Digest) {
			defer wg.Done()
			blobStart := time.Now()
			cw := &countWriter{writer: ioutil.Discard}
			err := ph.clusterClient.DownloadBlob(namespace, digest, cw)
			blobDuration := time.Since(blobStart)
			metrics.Timer("blob_download_time").Record(blobDuration)
			metrics.Counter("bytes_downloaded").Inc(cw.count)
			if err != nil {
				if serr, ok := err.(httputil.StatusError); ok && serr.Status == http.StatusAccepted {
					return
				}
				mu.Lock()
				errList = append(errList, fmt.Errorf("digest %s, namespace %s, error downloading blob: %w", digest, namespace, err))
				mu.Unlock()
			}
		}(d)
	}

	wg.Wait()

	if len(errList) > 0 {
		metrics.Counter("failed").Inc(1)
		for _, err := range errList {
			logger.With("error", err).Error("Error downloading blob")
		}
	}
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
func (ph *PrefetchHandler) processManifest(logger *zap.SugaredLogger, namespace string, manifestBytes []byte) ([]core.Digest, error) {
	// Attempt to process as a manifest list.
	digests, err := ph.tryProcessManifestList(logger, namespace, manifestBytes)
	if err == nil && len(digests) > 0 {
		return digests, nil
	}

	// Fallback to single manifest.
	var manifest schema2.Manifest
	if err := json.NewDecoder(bytes.NewReader(manifestBytes)).Decode(&manifest); err != nil {
		logger.With("namespace", namespace).Errorf("Failed to parse single manifest: %v", err)
		return nil, fmt.Errorf("invalid single manifest: %w", err)
	}
	return ph.processLayers(manifest.Layers)
}

// tryProcessManifestList attempts to decode a manifest list.
func (ph *PrefetchHandler) tryProcessManifestList(logger *zap.SugaredLogger, namespace string, manifestBytes []byte) ([]core.Digest, error) {
	var manifestList manifestlist.ManifestList
	if err := json.NewDecoder(bytes.NewReader(manifestBytes)).Decode(&manifestList); err != nil || len(manifestList.Manifests) == 0 {
		return nil, fmt.Errorf("not a valid manifest list")
	}
	logger.With("namespace", namespace).Info("Processing manifest list")
	return ph.processManifestList(logger, namespace, manifestList)
}

// processManifestList processes a manifest list.
func (ph *PrefetchHandler) processManifestList(logger *zap.SugaredLogger, namespace string, manifestList manifestlist.ManifestList) ([]core.Digest, error) {
	var allDigests []core.Digest
	for _, descriptor := range manifestList.Manifests {
		manifestDigestHex := descriptor.Digest.Hex()
		digest, err := core.NewSHA256DigestFromHex(manifestDigestHex)
		if err != nil {
			return nil, fmt.Errorf("failed to parse manifest digest %s: %w", manifestDigestHex, err)
		}
		buf := &bytes.Buffer{}
		if err := ph.clusterClient.DownloadBlob(namespace, digest, buf); err != nil {
			logger.Errorf("Failed to download manifest blob: %s", err)
			continue
		}
		var manifest schema2.Manifest
		if err := json.NewDecoder(buf).Decode(&manifest); err != nil {
			return nil, fmt.Errorf("failed to parse manifest: %w", err)
		}
		digests, err := ph.processLayers(manifest.Layers)
		if err != nil {
			return nil, err
		}
		allDigests = append(allDigests, digests...)
	}
	return allDigests, nil
}

// processLayers converts layer descriptors to a list of core.Digest.
func (ph *PrefetchHandler) processLayers(layers []distribution.Descriptor) ([]core.Digest, error) {
	digests := make([]core.Digest, 0, len(layers))
	for _, layer := range layers {
		digest, err := core.NewSHA256DigestFromHex(layer.Digest.Hex())
		if err != nil {
			return nil, fmt.Errorf("invalid layer digest: %w", err)
		}
		digests = append(digests, digest)
	}
	return digests, nil
}
