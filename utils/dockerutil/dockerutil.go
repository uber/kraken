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
	var refs []core.Digest
	for _, desc := range manifest.References() {
		d, err := core.ParseSHA256Digest(string(desc.Digest))
		if err != nil {
			return nil, fmt.Errorf("parse digest: %w", err)
		}
		refs = append(refs, d)
	}
	return refs, nil
}

// ParseOCIManifest parses an OCI image manifest v1.
// OCI manifests have the same structure as Docker v2 manifests, so we can parse them similarly.
func ParseOCIManifest(bytes []byte) (distribution.Manifest, core.Digest, error) {
	// First, try to parse as Docker v2 manifest (they have the same structure)
	// The Docker Distribution library might accept it if we use schema2.MediaTypeManifest
	manifest, desc, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, bytes)
	if err == nil {
		// Verify it's actually a schema2 manifest
		if _, ok := manifest.(*schema2.DeserializedManifest); ok {
			d, err := core.ParseSHA256Digest(string(desc.Digest))
			if err != nil {
				return nil, core.Digest{}, fmt.Errorf("parse digest: %s", err)
			}
			return manifest, d, nil
		}
	}

	// If that fails, parse manually by checking the mediaType in the JSON
	var manifestJSON struct {
		MediaType string `json:"mediaType"`
		Config    struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Layers []struct {
			Digest string `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal(bytes, &manifestJSON); err != nil {
		return nil, core.Digest{}, fmt.Errorf("unmarshal oci manifest json: %s", err)
	}

	if manifestJSON.MediaType != _ociManifestType {
		return nil, core.Digest{}, fmt.Errorf("expected oci manifest type %s, got %s", _ociManifestType, manifestJSON.MediaType)
	}

	// Calculate digest of the manifest
	d, err := core.NewDigester().FromBytes(bytes)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("calculate manifest digest: %s", err)
	}

	// Parse digests from OCI format (they're already in "sha256:hex" format)
	configDigest, err := core.ParseSHA256Digest(manifestJSON.Config.Digest)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("parse config digest: %s", err)
	}

	// Convert to Docker v2 format by creating a schema2 manifest
	// We need to convert the OCI structure to schema2 format
	configDesc := distribution.Descriptor{
		Digest:    digest.Digest(configDigest.String()),
		MediaType: schema2.MediaTypeImageConfig,
		Size:      0, // Size not available in OCI manifest
	}

	layers := make([]distribution.Descriptor, 0, len(manifestJSON.Layers))
	for _, layer := range manifestJSON.Layers {
		layerDigest, err := core.ParseSHA256Digest(layer.Digest)
		if err != nil {
			return nil, core.Digest{}, fmt.Errorf("parse layer digest: %s", err)
		}
		layers = append(layers, distribution.Descriptor{
			Digest:    digest.Digest(layerDigest.String()),
			MediaType: schema2.MediaTypeLayer,
			Size:      0, // Size not available in OCI manifest
		})
	}

	ociManifest := &schema2.DeserializedManifest{
		Manifest: schema2.Manifest{
			Versioned: manifestlist.SchemaVersion,
			Config:    configDesc,
			Layers:    layers,
		},
	}

	return ociManifest, d, nil
}

func GetSupportedManifestTypes() string {
	return fmt.Sprintf("%s,%s", _v2ManifestType, _v2ManifestListType)
}
