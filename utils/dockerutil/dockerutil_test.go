package dockerutil_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"testing/iotest"

	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/schema2"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/dockerutil"
)

func TestParseManifest(t *testing.T) {
	tests := map[string]struct {
		fixture       func() (core.Digest, []byte)
		wantErr       string
		wantMediaType string
	}{
		"success with v2 manifest": {
			fixture: func() (core.Digest, []byte) {
				return dockerutil.ManifestFixture(core.DigestFixture(), core.DigestFixture(), core.DigestFixture())
			},
			wantMediaType: schema2.MediaTypeManifest,
		},
		"success with v2 manifest list": {
			fixture: func() (core.Digest, []byte) {
				return dockerutil.ManifestListFixture(core.DigestFixture(), core.DigestFixture())
			},
			wantMediaType: manifestlist.MediaTypeManifestList,
		},
		"success with OCI manifest": {
			fixture: func() (core.Digest, []byte) {
				return dockerutil.OCIManifestFixture(core.DigestFixture(), core.DigestFixture(), core.DigestFixture())
			},
			wantMediaType: v1.MediaTypeImageManifest,
		},
		"success with OCI index": {
			fixture: func() (core.Digest, []byte) {
				return dockerutil.OCIIndexFixture(core.DigestFixture(), core.DigestFixture())
			},
			wantMediaType: v1.MediaTypeImageIndex,
		},
		"success with OCI manifest without mediaType": {
			fixture: func() (core.Digest, []byte) {
				raw := []byte(fmt.Sprintf(`{
					"schemaVersion": 2,
					"config": {"mediaType": "application/vnd.oci.image.config.v1+json", "size": 1, "digest": "%s"},
					"layers": [
						{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "size": 1, "digest": "%s"},
						{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "size": 1, "digest": "%s"}
					]
				}`, core.DigestFixture(), core.DigestFixture(), core.DigestFixture()))
				d, err := core.NewDigester().FromBytes(raw)
				if err != nil {
					panic(err)
				}
				return d, raw
			},
			wantMediaType: v1.MediaTypeImageManifest,
		},
		"success with OCI index without mediaType": {
			fixture: func() (core.Digest, []byte) {
				raw := []byte(fmt.Sprintf(`{
					"schemaVersion": 2,
					"manifests": [
						{"mediaType": "application/vnd.oci.image.manifest.v1+json", "size": 1, "digest": "%s", "platform": {"architecture": "amd64", "os": "linux"}},
						{"mediaType": "application/vnd.oci.image.manifest.v1+json", "size": 1, "digest": "%s", "platform": {"architecture": "arm64", "os": "linux"}}
					]
				}`, core.DigestFixture(), core.DigestFixture()))
				d, err := core.NewDigester().FromBytes(raw)
				if err != nil {
					panic(err)
				}
				return d, raw
			},
			wantMediaType: v1.MediaTypeImageIndex,
		},
		"failure with invalid JSON": {
			fixture: func() (core.Digest, []byte) { return core.Digest{}, []byte("not json") },
			wantErr: "peek manifest",
		},
		"failure when manifest schema version is not 2": {
			fixture: func() (core.Digest, []byte) {
				return core.Digest{}, []byte(`{"schemaVersion":1,"mediaType":"application/vnd.docker.distribution.manifest.v2+json"}`)
			},
			wantErr: "unsupported schema version: 1",
		},
		"failure when manifest is malformed": {
			fixture: func() (core.Digest, []byte) {
				return core.Digest{}, []byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","layers":"not-an-array"}`)
			},
			wantErr: "unmarshal manifest",
		},
		"failure with unknown mediatype": {
			fixture: func() (core.Digest, []byte) {
				return core.Digest{}, []byte(`{"schemaVersion":2,"mediaType":"application/unknown"}`)
			},
			wantErr: "unknown manifest mediatype: application/unknown",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fixtureD, fixtureB := tt.fixture()
			manifest, d, err := dockerutil.ParseManifest(bytes.NewReader(fixtureB))

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			mediaType, payload, err := manifest.Payload()
			require.NoError(t, err)
			require.Equal(t, tt.wantMediaType, mediaType)
			require.Equal(t, fixtureB, payload)
			require.Equal(t, fixtureD, d)
		})
	}
}

func TestParseManifest_ReaderError(t *testing.T) {
	giveReader := iotest.ErrReader(errors.New("test error"))
	_, _, err := dockerutil.ParseManifest(giveReader)
	require.ErrorContains(t, err, "read manifest")
}
