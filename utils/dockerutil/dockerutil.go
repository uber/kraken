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
package dockerutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/opencontainers/go-digest"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/log"
)

const (
	_v2ManifestType     = "application/vnd.docker.distribution.manifest.v2+json"
	_v2ManifestListType = "application/vnd.docker.distribution.manifest.list.v2+json"
	_ociManifestType    = "application/vnd.oci.image.manifest.v1+json"
)

func ParseManifest(r io.Reader) (distribution.Manifest, core.Digest, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("read: %s", err)
	}

	// Try Docker v2 manifest first
	manifest, d, err := ParseManifestV2(b)
	if err == nil {
		return manifest, d, err
	}

	// Try Docker v2 manifest list
	manifest, d, err = ParseManifestV2List(b)
	if err == nil {
		return manifest, d, err
	}

	// Try OCI manifest v1 (same structure as Docker v2)
	return ParseOCIManifest(b)
}

// ParseManifestV2 returns a parsed v2 manifest and its digest.
func ParseManifestV2(bytes []byte) (distribution.Manifest, core.Digest, error) {
	manifest, desc, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, bytes)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("unmarshal manifest: %s", err)
	}
	deserializedManifest, ok := manifest.(*schema2.DeserializedManifest)
	if !ok {
		return nil, core.Digest{}, errors.New("expected schema2.DeserializedManifest")
	}
	version := deserializedManifest.SchemaVersion
	if version != 2 {
		return nil, core.Digest{}, fmt.Errorf("unsupported manifest version: %d", version)
	}
	d, err := core.ParseSHA256Digest(string(desc.Digest))
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("parse digest: %s", err)
	}
	return manifest, d, nil
}

// ParseManifestV2List returns a parsed v2 manifest list and its digest.
func ParseManifestV2List(bytes []byte) (distribution.Manifest, core.Digest, error) {
	manifestList, desc, err := distribution.UnmarshalManifest(manifestlist.MediaTypeManifestList, bytes)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("unmarshal manifestlist: %s", err)
	}
	deserializedManifestList, ok := manifestList.(*manifestlist.DeserializedManifestList)
	if !ok {
		return nil, core.Digest{}, errors.New("expected manifestlist.DeserializedManifestList")
	}
	version := deserializedManifestList.SchemaVersion
	if version != 2 {
		return nil, core.Digest{}, fmt.Errorf("unsupported manifest list version: %d", version)
	}
	d, err := core.ParseSHA256Digest(string(desc.Digest))
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("parse digest: %s", err)
	}
	return manifestList, d, nil
}

// GetManifestReferences returns a list of references by a V2 manifest
func GetManifestReferences(manifest distribution.Manifest) ([]core.Digest, error) {
	refDescs := manifest.References()
	log.Debugw("GetManifestReferences called",
		"num_references", len(refDescs))

	var refs []core.Digest
	refDigestStrings := make([]string, 0, len(refDescs))
	for _, desc := range refDescs {
		d, err := core.ParseSHA256Digest(string(desc.Digest))
		if err != nil {
			return nil, fmt.Errorf("parse digest: %w", err)
		}
		refs = append(refs, d)
		refDigestStrings = append(refDigestStrings, d.String())
	}

	log.Debugw("GetManifestReferences returning",
		"num_digests", len(refs),
		"digests", refDigestStrings)

	return refs, nil
}

// ParseOCIManifest parses an OCI image manifest v1.
// OCI manifests have the same structure as Docker v2 manifests, so we convert them.
func ParseOCIManifest(bytes []byte) (distribution.Manifest, core.Digest, error) {
	// Parse the OCI manifest JSON to extract structure
	var manifestJSON struct {
		MediaType string `json:"mediaType"`
		Config    struct {
			Digest string `json:"digest"`
			Size   int64  `json:"size"`
		} `json:"config"`
		Layers []struct {
			Digest string `json:"digest"`
			Size   int64  `json:"size"`
		} `json:"layers"`
	}
	if err := json.Unmarshal(bytes, &manifestJSON); err != nil {
		return nil, core.Digest{}, fmt.Errorf("unmarshal oci manifest json: %s", err)
	}

	// Verify it's an OCI manifest
	if manifestJSON.MediaType != _ociManifestType {
		return nil, core.Digest{}, fmt.Errorf("expected oci manifest type %s, got %s", _ociManifestType, manifestJSON.MediaType)
	}

	log.Debugw("Parsing OCI manifest",
		"config_digest", manifestJSON.Config.Digest,
		"config_size", manifestJSON.Config.Size,
		"num_layers", len(manifestJSON.Layers))

	// Calculate digest of the original manifest
	d, err := core.NewDigester().FromBytes(bytes)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("calculate manifest digest: %s", err)
	}

	log.Debugw("OCI manifest digest calculated", "digest", d.String())

	// Parse digests from OCI format (they're already in "sha256:hex" format)
	configDigest, err := core.ParseSHA256Digest(manifestJSON.Config.Digest)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("parse config digest: %s", err)
	}

	// Convert to Docker v2 format by creating descriptors
	configDesc := distribution.Descriptor{
		Digest:    digest.Digest(configDigest.String()),
		MediaType: schema2.MediaTypeImageConfig,
		Size:      manifestJSON.Config.Size,
	}

	layers := make([]distribution.Descriptor, 0, len(manifestJSON.Layers))
	layerDigests := make([]string, 0, len(manifestJSON.Layers))
	for _, layer := range manifestJSON.Layers {
		layerDigest, err := core.ParseSHA256Digest(layer.Digest)
		if err != nil {
			return nil, core.Digest{}, fmt.Errorf("parse layer digest: %s", err)
		}
		layerDigests = append(layerDigests, layerDigest.String())
		layers = append(layers, distribution.Descriptor{
			Digest:    digest.Digest(layerDigest.String()),
			MediaType: schema2.MediaTypeLayer,
			Size:      layer.Size,
		})
	}

	log.Debugw("OCI manifest layers extracted",
		"layer_digests", layerDigests,
		"config_digest", configDigest.String())

	// Create a properly initialized schema2 manifest by converting OCI JSON to Docker v2 format
	// and then parsing it with the standard parser to ensure proper initialization
	// This is critical - manually constructing DeserializedManifest doesn't initialize
	// the internal state needed for References() to work correctly
	dockerV2JSON := struct {
		SchemaVersion int                       `json:"schemaVersion"`
		MediaType     string                    `json:"mediaType"`
		Config        distribution.Descriptor   `json:"config"`
		Layers        []distribution.Descriptor `json:"layers"`
	}{
		SchemaVersion: 2, // Docker v2 schema version
		MediaType:     schema2.MediaTypeManifest,
		Config:        configDesc,
		Layers:        layers,
	}

	dockerV2Bytes, err := json.Marshal(dockerV2JSON)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("marshal docker v2 manifest: %s", err)
	}

	// Parse it using the standard Docker v2 parser to ensure proper initialization
	manifest, _, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, dockerV2Bytes)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("unmarshal converted manifest: %s", err)
	}

	deserializedManifest, ok := manifest.(*schema2.DeserializedManifest)
	if !ok {
		return nil, core.Digest{}, errors.New("expected schema2.DeserializedManifest after conversion")
	}

	// Log what References() returns to verify it's working correctly
	refs := deserializedManifest.References()
	refDigests := make([]string, 0, len(refs))
	for _, ref := range refs {
		refDigests = append(refDigests, ref.Digest.String())
	}
	log.Debugw("OCI manifest converted to Docker v2, References() returned",
		"num_references", len(refs),
		"reference_digests", refDigests,
		"expected_config", configDigest.String(),
		"expected_layers", layerDigests)

	// Return the original OCI manifest's digest (calculated from original bytes)
	// This ensures we return the correct digest that matches what the registry expects
	return deserializedManifest, d, nil
}

func GetSupportedManifestTypes() string {
	return fmt.Sprintf("%s,%s,%s", _v2ManifestType, _v2ManifestListType, _ociManifestType)
}
