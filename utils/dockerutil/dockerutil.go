package dockerutil

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"code.uber.internal/infra/kraken/core"
	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/schema2"
)

// ParseManifestV2 returns a parsed v2 manifest and its digest
func ParseManifestV2(r io.Reader) (distribution.Manifest, core.Digest, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("read: %s", err)
	}
	manifest, desc, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, b)
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("unmarshal manifest: %s", err)
	}
	deserializedManifest, ok := manifest.(*schema2.DeserializedManifest)
	if !ok {
		return nil, core.Digest{}, errors.New("expected schema2.DeserializedManifest")
	}
	version := deserializedManifest.Manifest.Versioned.SchemaVersion
	if version != 2 {
		return nil, core.Digest{}, fmt.Errorf("unsupported manifest version: %d", version)
	}
	d, err := core.NewDigestFromString(string(desc.Digest))
	if err != nil {
		return nil, core.Digest{}, fmt.Errorf("parse digest: %s", err)
	}
	return manifest, d, nil
}

// GetManifestReferences returns a list of references by a V2 manifest
func GetManifestReferences(manifest distribution.Manifest) ([]core.Digest, error) {
	var refs []core.Digest
	for _, desc := range manifest.References() {
		d, err := core.NewDigestFromString(string(desc.Digest))
		if err != nil {
			return nil, fmt.Errorf("parse digest: %s", err)
		}
		refs = append(refs, d)
	}
	return refs, nil
}
