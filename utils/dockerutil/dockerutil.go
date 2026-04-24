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
	"errors"
	"fmt"
	"io"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema2"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/uber/kraken/core"
)

const (
	_v2ManifestType     = "application/vnd.docker.distribution.manifest.v2+json"
	_v2ManifestListType = "application/vnd.docker.distribution.manifest.list.v2+json"
)

func ParseManifest(r io.Reader) (distribution.Manifest, core.Digest, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("read: %s", err)
	}

	type attempt struct {
		name string
		fn   func([]byte) (distribution.Manifest, core.Digest, error)
	}
	attempts := []attempt{
		{"docker v2 manifest", ParseManifestV2},
		{"docker v2 manifest list", ParseManifestV2List},
		{"OCI image manifest", ParseManifestOCI},
		{"OCI image index", ParseManifestOCIIndex},
	}

	var errs []string
	for _, a := range attempts {
		m, d, err := a.fn(b)
		if err == nil {
			return m, d, nil
		}
		errs = append(errs, fmt.Sprintf("%s: %s", a.name, err))
	}
	return nil, core.Digest{}, fmt.Errorf("unrecognized manifest format: [%s]", strings.Join(errs, "; "))
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
	deserializedManifestIndex, ok := manifestIndex.(*manifestlist.DeserializedManifestList)
	if !ok {
		return nil, core.Digest{}, fmt.Errorf("expected OCI image index, got %T", manifestIndex)
	}
	version := deserializedManifestIndex.SchemaVersion
	if version != 2 {
		return nil, core.Digest{}, fmt.Errorf("unsupported OCI image index version: %d", version)
	}
	d, err := core.ParseSHA256Digest(string(desc.Digest))
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("parse digest: %s", err)
	}
	return manifestList, d, nil
}

// ParseManifestOCI returns a parsed OCI image manifest and its digest.
func ParseManifestOCI(bytes []byte) (distribution.Manifest, core.Digest, error) {
	manifest, desc, err := distribution.UnmarshalManifest(specs.MediaTypeImageManifest, bytes)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("unmarshal OCI manifest: %s", err)
	}
	_, ok := manifest.(*ocischema.DeserializedManifest)
	if !ok {
		return nil, core.Digest{}, errors.New("expected ocischema.DeserializedManifest")
	}
	d, err := core.ParseSHA256Digest(string(desc.Digest))
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("parse digest: %s", err)
	}
	return manifest, d, nil
}

// ParseManifestOCIIndex returns a parsed OCI image index and its digest.
func ParseManifestOCIIndex(bytes []byte) (distribution.Manifest, core.Digest, error) {
	manifestIndex, desc, err := distribution.UnmarshalManifest(specs.MediaTypeImageIndex, bytes)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("unmarshal OCI image index: %s", err)
	}
	_, ok := manifestIndex.(*manifestlist.DeserializedManifestList)
	if !ok {
		return nil, core.Digest{}, errors.New("expected manifestlist.DeserializedManifestList for OCI index")
	}
	d, err := core.ParseSHA256Digest(string(desc.Digest))
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("parse digest: %s", err)
	}
	return manifestIndex, d, nil
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

func GetSupportedManifestTypes() string {
	return fmt.Sprintf("%s,%s,%s,%s",
		_v2ManifestType,
		_v2ManifestListType,
		specs.MediaTypeImageManifest,
		specs.MediaTypeImageIndex,
	)
}