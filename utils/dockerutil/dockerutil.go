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
package dockerutil

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/uber/kraken/core"
)

// ParseManifestV2 returns a parsed v2 manifest and its digest
func ParseManifestV2(r io.Reader) (manifest distribution.Manifest, digest core.Digest, err error) {
	manifest, digest, err = ParseManifest(schema2.MediaTypeManifest, r)
	if err != nil {
		return
	}

	deserializedManifest, ok := manifest.(*schema2.DeserializedManifest)
	if !ok {
		return nil, core.Digest{}, errors.New("expected schema2.DeserializedManifest")
	}
	version := deserializedManifest.Manifest.Versioned.SchemaVersion
	if version != 2 {
		return nil, core.Digest{}, fmt.Errorf("unsupported manifest version: %d", version)
	}

	return
}

// ParseManifest returns a parsed manifest and its digest
func ParseManifest(contentType string, r io.Reader) (distribution.Manifest, core.Digest, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("read: %s", err)
	}
	manifest, desc, err := distribution.UnmarshalManifest(contentType, b)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("unmarshal manifest: %s", err)
	}
	d, err := core.ParseSHA256Digest(string(desc.Digest))
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("parse digest: %s", err)
	}
	return manifest, d, nil
}

// GetManifestReferences returns a list of references by a V2 manifest
func GetManifestReferences(manifest distribution.Manifest) ([]core.Digest, error) {
	var refs []core.Digest
	for _, desc := range manifest.References() {
		d, err := core.ParseSHA256Digest(string(desc.Digest))
		if err != nil {
			return nil, fmt.Errorf("parse digest: %s", err)
		}
		refs = append(refs, d)
	}
	return refs, nil
}
