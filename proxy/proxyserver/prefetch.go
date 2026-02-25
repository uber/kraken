package proxyserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	clusterClient    blobclient.ClusterClient
	tagClient        tagclient.Client
	tagParser        TagParser
	minBlobSizeBytes int64 // Minimum size in bytes for a blob to be prefetched. 0 means no minimum.
	maxBlobSizeBytes int64 // Maximum size in bytes for a blob to be prefetched. 0 means no maximum.
	v1Synchronous    bool

	metrics            tally.Scope
	getManifestLatency tally.Histogram
	getTagLatency      tally.Histogram
}

// blobInfo holds digest and size information for a blob.
type blobInfo struct {
	digest core.Digest
	size   int64
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

type TagParser interface {
	ParseTag(tag string) (namespace, name string, err error)
}

type DefaultTagParser struct{}

// ParseTag implements the TagParser interface.
// Expects tag strings in the format <hostname>/<namespace>/<imagename:tag>.
func (p *DefaultTagParser) ParseTag(tag string) (namespace, name string, err error) {
	parts := strings.Split(tag, "/")
	if len(parts) < 3 {
		return "", "", fmt.Errorf("invalid tag format: %s", tag)
	}
	return parts[1], parts[2], nil
}

// NewPrefetchHandler constructs a new PrefetchHandler.
func NewPrefetchHandler(
	client blobclient.ClusterClient,
	tagClient tagclient.Client,
	tagParser TagParser,
	metrics tally.Scope,
	minBlobSizeBytes int64,
	maxBlobSizeBytes int64,
	v1Synchronous bool,
) *PrefetchHandler {
	if tagParser == nil {
		tagParser = &DefaultTagParser{}
	}
	// Apply defaults if not configured
	if minBlobSizeBytes == 0 {
		minBlobSizeBytes = int64(DefaultPrefetchMinBlobSize)
	}
	if maxBlobSizeBytes == 0 {
		maxBlobSizeBytes = int64(DefaultPrefetchMaxBlobSize)
	}
	m := metrics.SubScope("prefetch")
	return &PrefetchHandler{
		clusterClient:    client,
		tagClient:        tagClient,
		tagParser:        tagParser,
		v1Synchronous:    v1Synchronous,
		minBlobSizeBytes: minBlobSizeBytes,
		maxBlobSizeBytes: maxBlobSizeBytes,

		metrics:            m,
		getManifestLatency: m.Histogram("download_manifest_latency", tally.MustMakeExponentialDurationBuckets(1*time.Second, 2, 12)),
		getTagLatency:      m.Histogram("get_tag_latency", tally.MustMakeExponentialDurationBuckets(100*time.Millisecond, 2, 10)),
	}
}

// newPrefetchSuccessResponse constructs a successful response.
func newPrefetchSuccessResponse(tag, msg, traceId string) *prefetchResponse {
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
	w.Header().Set("Content-Type", "application/json")

	response, err := json.Marshal(payload)
	if err != nil {
		log.With("payload", payload).Errorf("Failed to marshal JSON: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Write the status code and the encoded JSON response.
	w.WriteHeader(status)
	if _, err := w.Write(response); err != nil {
		log.With("payload", payload).Errorf("Failed to write response: %v", err)
	}
}

func writeBadRequestError(w http.ResponseWriter, msg, traceId string) {
	writeJSON(w, http.StatusBadRequest, newPrefetchError(http.StatusBadRequest, msg, traceId))
}

func writeInternalError(w http.ResponseWriter, msg, traceId string) {
	writeJSON(w, http.StatusInternalServerError, newPrefetchError(http.StatusInternalServerError, msg, traceId))
}

func writePrefetchResponse(w http.ResponseWriter, tag, msg, traceId string) {
	writeJSON(w, http.StatusOK, newPrefetchSuccessResponse(tag, msg, traceId))
}

// HandleV1 processes the prefetch request.
func (ph *PrefetchHandler) HandleV1(w http.ResponseWriter, r *http.Request) {
	input, errOccurred := ph.preparePrefetch(w, r)
	if errOccurred {
		return
	}

	ph.metrics.Counter("initiated").Inc(1)
	writePrefetchResponse(w, input.tag, "prefetching initiated successfully", input.traceID)

	if ph.v1Synchronous {
		ph.downloadBlobs(input)
	} else {
		// Download blobs asynchronously.
		go ph.downloadBlobs(input)
	}
}

type prefetchInput struct {
	blobs     []blobInfo
	namespace string
	logger    *zap.SugaredLogger
	tag       string
	traceID   string
}

// preparePrefetch parses the request, calls build-index to get the image manifest SHA,
// downloads the manifest(s) from the origin cluster, parses them, and returns the blobs layers to prefetch.
// If an error occurs, preparePrefetch returns the appropriate HTTP response.
func (ph *PrefetchHandler) preparePrefetch(w http.ResponseWriter, r *http.Request) (res *prefetchInput, errOccurred bool) {
	ph.metrics.Counter("requests").Inc(1)
	var reqBody prefetchBody
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		writeBadRequestError(w, fmt.Sprintf("failed to decode request body: %s", err), "")
		log.With("error", err).Error("Failed to decode request body")
		return nil, true
	}
	logger := log.
		With("trace_id", reqBody.TraceId).
		With("image_tag", reqBody.Tag)

	namespace, tag, err := ph.tagParser.ParseTag(reqBody.Tag)
	if err != nil {
		writeBadRequestError(w, fmt.Sprintf("tag: %s, invalid tag format: %s", reqBody.Tag, err), reqBody.TraceId)
		return nil, true
	}

	tagRequest := url.QueryEscape(fmt.Sprintf("%s/%s", namespace, tag))
	startTime := time.Now()
	digest, err := ph.tagClient.Get(tagRequest)
	if err != nil {
		ph.metrics.Counter("get_tag_error").Inc(1)
		logger.With("error", err).Error("Failed to get manifest tag")
		writeInternalError(w, fmt.Sprintf("tag request: %s, failed to get tag: %s", tagRequest, err), reqBody.TraceId)
		return nil, true
	}
	ph.getTagLatency.RecordDuration(time.Since(startTime))
	logger.Infof("Namespace: %s, Tag: %s", namespace, tag)

	buf := &bytes.Buffer{}
	startTime = time.Now()
	if err := ph.clusterClient.DownloadBlob(context.Background(), namespace, digest, buf); err != nil {
		ph.metrics.Counter("download_manifest_error").Inc(1)
		logger.With("error", err).Error("Failed to download manifest blob")
		writeInternalError(w, fmt.Sprintf("error downloading manifest blob: %s", err), reqBody.TraceId)
		return nil, true
	}
	ph.getManifestLatency.RecordDuration(time.Since(startTime))

	// Process manifest (ManifestList or single Manifest)
	blobs, err := ph.processManifest(logger, namespace, buf.Bytes())
	if err != nil {
		writeInternalError(w, fmt.Sprintf("failed to process manifest: %s", err), reqBody.TraceId)
		return nil, true
	}

	return &prefetchInput{
		blobs:     blobs,
		namespace: namespace,
		logger:    logger,
		tag:       tag,
		traceID:   reqBody.TraceId,
	}, false
}

// downloadBlobs downloads blobs in parallel.
func (ph *PrefetchHandler) downloadBlobs(input *prefetchInput) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errList []error

	for _, b := range input.blobs {
		if ph.shouldSkipPrefetch(b, input.logger) {
			continue
		}

		wg.Add(1)
		go func(blob blobInfo) {
			defer wg.Done()
			blobStart := time.Now()
			err := ph.clusterClient.DownloadBlob(context.Background(), input.namespace, blob.digest, io.Discard)
			blobDuration := time.Since(blobStart)
			ph.metrics.Timer("blob_download_time").Record(blobDuration)
			ph.metrics.Counter("bytes_downloaded").Inc(blob.size)
			if err != nil {
				if serr, ok := err.(httputil.StatusError); ok && serr.Status == http.StatusAccepted {
					return
				}
				mu.Lock()
				errList = append(errList, fmt.Errorf("digest %s, namespace %s, error downloading blob: %w", blob.digest, input.namespace, err))
				mu.Unlock()
			} else {
				ph.metrics.Counter("blobs_downloaded").Inc(1)
			}
		}(b)
	}

	wg.Wait()

	if len(errList) > 0 {
		ph.metrics.Counter("failed").Inc(1)
		for _, err := range errList {
			input.logger.With("error", err).Error("Error downloading blob")
		}
	}
}

// Skip blobs that are outside the size range [min, max]
func (ph *PrefetchHandler) shouldSkipPrefetch(b blobInfo, logger *zap.SugaredLogger) bool {
	if b.size < ph.minBlobSizeBytes {
		logger.With(
			"digest", b.digest,
			"size", b.size,
			"min_threshold", ph.minBlobSizeBytes,
		).Infof("Skipping blob: size below minimum threshold")
		ph.metrics.Counter("blobs_skipped_too_small").Inc(1)
		return true
	}
	if b.size > ph.maxBlobSizeBytes {
		logger.With(
			"digest", b.digest,
			"size", b.size,
			"max_threshold", ph.maxBlobSizeBytes,
		).Infof("Skipping blob: size exceeds maximum threshold")
		ph.metrics.Counter("blobs_skipped_too_large").Inc(1)
		return true
	}
	return false
}

// processManifest handles both ManifestLists and single Manifests.
func (ph *PrefetchHandler) processManifest(logger *zap.SugaredLogger, namespace string, manifestBytes []byte) ([]blobInfo, error) {
	// Attempt to process as a manifest list.
	blobs, err := ph.tryProcessManifestList(logger, namespace, manifestBytes)
	if err == nil && len(blobs) > 0 {
		return blobs, nil
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
func (ph *PrefetchHandler) tryProcessManifestList(logger *zap.SugaredLogger, namespace string, manifestBytes []byte) ([]blobInfo, error) {
	var manifestList manifestlist.ManifestList
	if err := json.NewDecoder(bytes.NewReader(manifestBytes)).Decode(&manifestList); err != nil || len(manifestList.Manifests) == 0 {
		return nil, fmt.Errorf("not a valid manifest list")
	}
	logger.With("namespace", namespace).Info("Processing manifest list")
	return ph.processManifestList(logger, namespace, manifestList)
}

// processManifestList processes a manifest list.
func (ph *PrefetchHandler) processManifestList(logger *zap.SugaredLogger, namespace string, manifestList manifestlist.ManifestList) ([]blobInfo, error) {
	var allBlobs []blobInfo
	for _, descriptor := range manifestList.Manifests {
		manifestDigestHex := descriptor.Digest.Hex()
		digest, err := core.NewSHA256DigestFromHex(manifestDigestHex)
		if err != nil {
			return nil, fmt.Errorf("failed to parse manifest digest %s: %w", manifestDigestHex, err)
		}
		buf := &bytes.Buffer{}
		startTime := time.Now()
		if err := ph.clusterClient.DownloadBlob(context.Background(), namespace, digest, buf); err != nil {
			ph.metrics.Counter("download_manifest_error").Inc(1)
			logger.With("error", err).Error("Failed to download manifest blob")
			continue
		}
		ph.getManifestLatency.RecordDuration(time.Since(startTime))
		var manifest schema2.Manifest
		if err := json.NewDecoder(buf).Decode(&manifest); err != nil {
			return nil, fmt.Errorf("failed to parse manifest: %w", err)
		}
		blobs, err := ph.processLayers(manifest.Layers)
		if err != nil {
			return nil, err
		}
		allBlobs = append(allBlobs, blobs...)
	}
	return allBlobs, nil
}

// processLayers converts layer descriptors to a list of blobInfo with size information.
func (ph *PrefetchHandler) processLayers(layers []distribution.Descriptor) ([]blobInfo, error) {
	blobs := make([]blobInfo, 0, len(layers))
	for _, layer := range layers {
		digest, err := core.NewSHA256DigestFromHex(layer.Digest.Hex())
		if err != nil {
			return nil, fmt.Errorf("invalid layer digest: %w", err)
		}
		blobs = append(blobs, blobInfo{
			digest: digest,
			size:   layer.Size,
		})
	}
	return blobs, nil
}

// HandleV2 is a *mostly* idempotent operation that preheats the origin cluster's cache with the provided image.
// For each image layer:
// - if it is not present, it is prefetched by the origins asynchronously.
// - if it is present, no-op.
// The operation is "mostly" idempotent, as while it does not cause image layer redownloads,
// it ALWAYS 1) calls BI to get the manifest SHA and 2) downloads all image manifests from the origins.
func (ph *PrefetchHandler) HandleV2(w http.ResponseWriter, r *http.Request) {
	input, errOccurred := ph.preparePrefetch(w, r)
	if errOccurred {
		return
	}

	err := ph.triggerPrefetchBlobs(input)
	if err != nil {
		writeInternalError(w, fmt.Sprintf("failed to trigger image prefetch: %s", err), input.traceID)
		input.logger.Errorf("Failed to trigger image prefetch")
		return
	}

	ph.metrics.Counter("initiated").Inc(1)
	writePrefetchResponse(w, input.tag, "prefetching initiated successfully", input.traceID)
}

// triggerPrefetchBlobs triggers a blob prefetch for all blobs in parallel.
func (ph *PrefetchHandler) triggerPrefetchBlobs(input *prefetchInput) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errList []error

	for _, b := range input.blobs {
		if ph.shouldSkipPrefetch(b, input.logger) {
			continue
		}

		wg.Add(1)
		go func(digest core.Digest) {
			defer wg.Done()
			err := ph.clusterClient.PrefetchBlob(input.namespace, digest)
			if err != nil {
				mu.Lock()
				errList = append(errList, fmt.Errorf("digest %q, namespace %q, blob prefetch failure: %w", digest, input.namespace, err))
				mu.Unlock()
			}
		}(b.digest)
	}
	wg.Wait()

	if len(errList) != 0 {
		return fmt.Errorf("at least one layer could not be prefetched: %w", errors.Join(errList...))
	}
	return nil
}
