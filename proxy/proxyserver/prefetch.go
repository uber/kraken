package proxyserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
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

// this is mounted from the host's `/opt/uber/jobcontroller/kraken-proxy/default/mnt/data/prefetch_config.json`
const _configFilePath = "/var/cache/udocker/kraken-proxy/prefetch_config.json"

type PrefetchHyperparams struct {
	MinBlobSizeBytes int64 `json:"min_blob_size_bytes"`
	MaxBlobSizeBytes int64 `json:"max_blob_size_bytes"`
}

// PrefetchHandler handles prefetch requests.
type PrefetchHandler struct {
	clusterClient    blobclient.ClusterClient
	tagClient        tagclient.Client
	tagParser        TagParser
	metrics          tally.Scope
	synchronous      bool
	minBlobSizeBytes int64 // Minimum size in bytes for a blob to be prefetched. 0 means no minimum.
	maxBlobSizeBytes int64 // Maximum size in bytes for a blob to be prefetched. 0 means no maximum.
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

// optionally adjust the config without redeploying
func overwriteConfig(min, max int64, logger *zap.SugaredLogger) (resMin, resMax int64) {
	configFile, err := os.Open(_configFilePath)
	if err != nil {
		logger.Infof("Failed to open dynamic proxy prefetch config file at %s: %v", _configFilePath, err)
		fmt.Printf("Failed to open dynamic proxy prefetch config file at %s: %v", _configFilePath, err)
		return min, max
	}

	defer configFile.Close()
	configData, err := io.ReadAll(configFile)
	if err != nil {
		logger.Infof("Failed to read proxy prefetch config file: %v", err)
		fmt.Printf("Failed to read proxy prefetch config file: %v", err)
		return min, max
	}

	var hyperparams PrefetchHyperparams
	if err := json.Unmarshal(configData, &hyperparams); err != nil {
		logger.Infof("Failed to unmarshal proxy prefetch hyperparameters: %v", err)
		fmt.Printf("Failed to unmarshal proxy prefetch hyperparameters: %v", err)
		return min, max
	}

	logger.Infof("Loading proxy prefetch hyperparameters: MinBlobSizeBytes=%d, MaxBlobSizeBytes=%d",
		hyperparams.MinBlobSizeBytes, hyperparams.MaxBlobSizeBytes)
	fmt.Printf("Loading proxy prefetch hyperparameters: MinBlobSizeBytes=%d, MaxBlobSizeBytes=%d",
		hyperparams.MinBlobSizeBytes, hyperparams.MaxBlobSizeBytes)
	return hyperparams.MinBlobSizeBytes, hyperparams.MaxBlobSizeBytes
}

// NewPrefetchHandler constructs a new PrefetchHandler.
func NewPrefetchHandler(
	client blobclient.ClusterClient,
	tagClient tagclient.Client,
	tagParser TagParser,
	metrics tally.Scope,
	synchronous bool,
	minBlobSizeBytes int64,
	maxBlobSizeBytes int64,
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

	logger := log.With("dynamic", "true")
	minBlobSizeBytes, maxBlobSizeBytes = overwriteConfig(minBlobSizeBytes, maxBlobSizeBytes, logger)

	return &PrefetchHandler{
		clusterClient:    client,
		tagClient:        tagClient,
		tagParser:        tagParser,
		metrics:          metrics.SubScope("prefetch"),
		synchronous:      synchronous,
		minBlobSizeBytes: minBlobSizeBytes,
		maxBlobSizeBytes: maxBlobSizeBytes,
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

	namespace, tag, err := ph.tagParser.ParseTag(reqBody.Tag)
	if err != nil {
		writeBadRequestError(w, fmt.Sprintf("tag: %s, invalid tag format: %s", reqBody.Tag, err), reqBody.TraceId)
		return
	}

	tagRequest := url.QueryEscape(fmt.Sprintf("%s/%s", namespace, tag))
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
	blobs, err := ph.processManifest(logger, namespace, buf.Bytes())
	if err != nil {
		writeInternalError(w, fmt.Sprintf("failed to process manifest: %s", err), reqBody.TraceId)
		return
	}

	ph.metrics.SubScope("prefetch").Counter("initiated").Inc(1)
	writePrefetchResponse(w, reqBody.Tag, "prefetching initiated successfully", reqBody.TraceId)

	if ph.synchronous {
		ph.prefetchBlobs(logger, namespace, blobs)
	} else {
		// Prefetch blobs asynchronously.
		go ph.prefetchBlobs(logger, namespace, blobs)
	}
}

// prefetchBlobs downloads blobs in parallel, filtering by size threshold.
func (ph *PrefetchHandler) prefetchBlobs(logger *zap.SugaredLogger, namespace string, blobs []blobInfo) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errList []error

	for _, b := range blobs {
		// Skip blobs that are outside the size range [min, max]
		if b.size < ph.minBlobSizeBytes {
			logger.With(
				"digest", b.digest,
				"size", b.size,
				"min_threshold", ph.minBlobSizeBytes,
			).Infof("Skipping blob: size below minimum threshold")
			ph.metrics.Counter("blobs_skipped_too_small").Inc(1)
			continue
		}
		if b.size > ph.maxBlobSizeBytes {
			logger.With(
				"digest", b.digest,
				"size", b.size,
				"max_threshold", ph.maxBlobSizeBytes,
			).Infof("Skipping blob: size exceeds maximum threshold")
			ph.metrics.Counter("blobs_skipped_too_large").Inc(1)
			continue
		}

		wg.Add(1)
		go func(blob blobInfo) {
			defer wg.Done()
			blobStart := time.Now()
			err := ph.clusterClient.DownloadBlob(namespace, blob.digest, io.Discard)
			blobDuration := time.Since(blobStart)
			ph.metrics.Timer("blob_download_time").Record(blobDuration)
			ph.metrics.Counter("bytes_downloaded").Inc(blob.size)
			if err != nil {
				if serr, ok := err.(httputil.StatusError); ok && serr.Status == http.StatusAccepted {
					return
				}
				mu.Lock()
				errList = append(errList, fmt.Errorf("digest %s, namespace %s, error downloading blob: %w", blob.digest, namespace, err))
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
			logger.With("error", err).Error("Error downloading blob")
		}
	}
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
		if err := ph.clusterClient.DownloadBlob(namespace, digest, buf); err != nil {
			logger.Errorf("Failed to download manifest blob: %s", err)
			continue
		}
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
