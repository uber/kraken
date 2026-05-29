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
	"fmt"
	"io"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/manifestlist"
	_ "github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema2"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/uber/kraken/core"
)

type manifestPeek struct {
	SchemaVersion int               `json:"schemaVersion"`
	MediaType     string            `json:"mediaType"`
	Manifests     []json.RawMessage `json:"manifests"`
}

func ParseManifest(r io.Reader) (distribution.Manifest, core.Digest, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("read manifest: %w", err)
	}

	var p manifestPeek
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, core.Digest{}, fmt.Errorf("peek manifest: %w", err)
	}
	if p.SchemaVersion != 2 {
		return nil, core.Digest{}, fmt.Errorf("unsupported schema version: %d", p.SchemaVersion)
	}
	// OCI manifests may omit their mediatype. Use the "manifests" field
	// to differentiate between an OCI manifest and an OCI index.
	if p.MediaType == "" {
		if len(p.Manifests) > 0 {
			p.MediaType = v1.MediaTypeImageIndex
		} else {
			p.MediaType = v1.MediaTypeImageManifest
		}
	}

	switch p.MediaType {
	case schema2.MediaTypeManifest, manifestlist.MediaTypeManifestList, v1.MediaTypeImageManifest, v1.MediaTypeImageIndex:
		return unmarshalManifest(p.MediaType, b)
	default:
		return nil, core.Digest{}, fmt.Errorf("unknown manifest mediatype: %s", p.MediaType)
	}
}

func unmarshalManifest(mediaType string, b []byte) (distribution.Manifest, core.Digest, error) {
	manifest, desc, err := distribution.UnmarshalManifest(mediaType, b)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("unmarshal manifest: %w", err)
	}
	digest, err := core.ParseSHA256Digest(string(desc.Digest))
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("parse digest: %w", err)
	}
	return manifest, digest, nil
}

// GetManifestReferences returns a list of references by a manifest.
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

func GetSupportedManifestTypes() string {
	return fmt.Sprintf("%s,%s,%s,%s",
		schema2.MediaTypeManifest,
		manifestlist.MediaTypeManifestList,
		v1.MediaTypeImageManifest,
		v1.MediaTypeImageIndex)
}
