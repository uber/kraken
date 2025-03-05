package proxyserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/log"
	"io/ioutil"
	"net/http"
	"strings"
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
}

// prefetchBody defines the body of preheat.
type prefetchBody struct {
	// Tag is the tag of the image. The format api expect is: address/namespace/tag.
	// Example: 127.0.0.1:5055/uber-usi/abacus-go:bkt1-produ-1700103798-0154d
	Tag string `json:"tag"`
}

// NewPrefetchHandler creates a new preheat handler.
func NewPrefetchHandler(client blobclient.ClusterClient, tagClient tagclient.Client) *PrefetchHandler {
	return &PrefetchHandler{client, tagClient}
}

// Handle processes the prefetch request.
func (ph *PrefetchHandler) Handle(w http.ResponseWriter, r *http.Request) error {
	var reqBody prefetchBody
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		bodyBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return handler.Errorf("failed to read body: %s", err)
		}
		log.With("error", err, "body", string(bodyBytes)).Error("Failed to decode request body")
		return handler.Errorf("failed to decode body: %s", err)
	}

	namespace, tag, err := parseTag(reqBody.Tag)
	if err != nil {
		return handler.Errorf("tag: %s, invalid tag format: %s", reqBody.Tag, err)
	}

	tagRequest := fmt.Sprintf("%s%%2F%s", namespace, tag)
	digest, err := ph.tagClient.Get(tagRequest)
	if err != nil {
		return handler.Errorf("tag request: %s, failed to get tag: %s", tagRequest, err)
	}

	stat, err := ph.clusterClient.Stat(namespace, digest)
	if err != nil {
		return handler.Errorf("failed to get meta info: %w", err)
	}
	log.Infof("Namespace: %s, Tag: %s, Size: %d", namespace, tag, stat.Size)

	buf := &bytes.Buffer{}
	if err := ph.clusterClient.DownloadBlob(namespace, digest, buf); err != nil {
		return handler.Errorf("error downloading manifest blob: %s", err)
	}

	// Process manifest (ManifestList or single Manifest)
	size, digests, err := ph.processManifest(namespace, buf)
	if err != nil {
		return handler.Errorf("failed to process manifest: %s", err)
	}
	size += stat.Size

	// todo: move to config
	if size <= 5_000_000_000 {
		for _, digest := range digests {
			buf.Reset()
			// todo: no need to download into the buffer
			if err := ph.clusterClient.DownloadBlob(namespace, digest, buf); err != nil {
				return handler.Errorf("digest %s, namespace %s, error downloading blob: %s", digest, namespace, err)
			}
		}
	} else {
		log.With("size", size, "tag", reqBody.Tag).Info("Size is too large, skipping preheat")
	}
	return nil
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
func (ph *PrefetchHandler) processManifest(namespace string, buf *bytes.Buffer) (int64, []core.Digest, error) {
	reader := bytes.NewReader(buf.Bytes())
	// Try to decode as ManifestList
	var manifestList manifestlist.ManifestList
	if err := json.NewDecoder(reader).Decode(&manifestList); err == nil && len(manifestList.Manifests) > 0 {
		log.With("namespace", namespace).Info("Processing manifest list...")
		return ph.processManifestList(namespace, manifestList)
	}

	_, err := reader.Seek(0, 0)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to shift the buffer offset: %s", err)
	}
	var manifest schema2.Manifest
	if err := json.NewDecoder(reader).Decode(&manifest); err != nil {
		content, err := ioutil.ReadAll(reader)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to read single manifest: %s", err)
		}
		log.With("namespace", namespace).Errorf("Failed to decode single manifest %s", string(content))
		return 0, nil, fmt.Errorf("failed to parse single manifest: %s", err)
	}

	return ph.processLayers(namespace, manifest.Layers)
}

// processManifestList iterates over the manifest list and processes each entry.
func (ph *PrefetchHandler) processManifestList(namespace string, manifestList manifestlist.ManifestList) (int64, []core.Digest, error) {
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
			log.Errorf("Failed to download manifest blob: %s", err)
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

// processLayers downloads all layers in a manifest.
func (ph *PrefetchHandler) processLayers(namespace string, layers []distribution.Descriptor) (int64, []core.Digest, error) {
	digests := make([]core.Digest, 0, len(layers))
	size := int64(0)
	for _, layer := range layers {
		digest, err := core.NewSHA256DigestFromHex(layer.Digest.Hex())
		if err != nil {
			return 0, nil, fmt.Errorf("failed to parse layer digest: %w", err)
		}

		stat, err := ph.clusterClient.Stat(namespace, digest)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to get meta info: %w", err)
		}
		size += stat.Size
		digests = append(digests, digest)
	}
	return size, digests, nil
}
