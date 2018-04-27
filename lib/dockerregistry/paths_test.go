package dockerregistry

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlobsPath(t *testing.T) {
	testCases := []struct {
		input  string
		digest string
		err    error
	}{
		{"", "", InvalidRegistryPathError{_blobs, ""}},
		{"/v2/blobs/sha256/1234/digest5678/data", "", InvalidRegistryPathError{_blobs, "/v2/blobs/sha256/1234/digest5678/data"}},
		{"/v2/blobs/sha256/1234/digest5678", "", InvalidRegistryPathError{_blobs, "/v2/blobs/sha256/1234/digest5678"}},
		{"/v2/blobs/sha256/3Z/digest5678/data", "", InvalidRegistryPathError{_blobs, "/v2/blobs/sha256/3Z/digest5678/data"}},
		{"/v2/blobs/sha256/3z/digest5678/data", "digest5678", nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetBlobDigest:%s", tc.input), func(t *testing.T) {
			require := require.New(t)
			digest, err := GetBlobDigest(tc.input)
			require.Equal(tc.digest, digest)
			require.Equal(tc.err, err)
		})
	}
}

func TestRepositoriesPath(t *testing.T) {
	testCases := []struct {
		input string
		repo  string
		err   error
	}{
		{"", "", InvalidRegistryPathError{_repositories, ""}},
		{"/v2/repositories/_manifests", "", InvalidRegistryPathError{_repositories, "/v2/repositories/_manifests"}},
		{"/v2/repo/kraken/_manifests", "", InvalidRegistryPathError{_repositories, "/v2/repo/kraken/_manifests"}},
		{"/v2/repositories/kraken/versions", "", InvalidRegistryPathError{_repositories, "/v2/repositories/kraken/versions"}},
		{"/v2/repositories/kraken/_manifests", "kraken", nil},
		{"/v2/repositories/kraken/_uploads", "kraken", nil},
		{"/v2/repositories/kraken/_layers", "kraken", nil},
		{"/v2/repositories/uber-usi/kraken/_manifests", "uber-usi/kraken", nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetRepositoriesRepo %s", tc.input), func(t *testing.T) {
			require := require.New(t)
			repo, err := GetRepo(tc.input)
			require.Equal(tc.repo, repo)
			require.Equal(tc.err, err)
		})
	}
}

func TestLayersPath(t *testing.T) {
	testCases := []struct {
		input   string
		match   bool
		subtype PathSubType
	}{
		{"", false, _invalidPathSubType},
		{"kraken/_layers/digest5678/", false, _invalidPathSubType},
		{"kraken/_layers/sha256/digest5678/", false, _invalidPathSubType},
		{"kraken/_layers/sha256/digestZ5678/", false, _invalidPathSubType},
		{"kraken/_layers/sha256/digest5678/data", true, _data},
		{"kraken/_layers/sha256/digest5678/link", true, _link},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("matchLayersPath %s", tc.input), func(t *testing.T) {
			require := require.New(t)
			match, subtype := matchLayersPath(tc.input)
			require.Equal(tc.match, match)
			require.Equal(tc.subtype, subtype)
		})
	}
}

func TestLayersPathGetDigest(t *testing.T) {
	testCases := []struct {
		input  string
		digest string
		err    error
	}{
		{"", "", InvalidRegistryPathError{_layers, ""}},
		{"kraken/_layers/digest5678/", "", InvalidRegistryPathError{_layers, "kraken/_layers/digest5678/"}},
		{"kraken/_layers/sha256/digest5678/", "", InvalidRegistryPathError{_layers, "kraken/_layers/sha256/digest5678/"}},
		{"kraken/_layers/sha256/digest5678/data", "digest5678", nil},
		{"kraken/_layers/sha256/digest5678/link", "digest5678", nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetLayerDigest:%s", tc.input), func(t *testing.T) {
			require := require.New(t)
			digest, err := GetLayerDigest(tc.input)
			require.Equal(tc.digest, digest)
			require.Equal(tc.err, err)
		})
	}
}

func TestManifestsPathMatch(t *testing.T) {
	testCases := []struct {
		input   string
		match   bool
		subtype PathSubType
	}{
		{"", false, _invalidPathSubType},
		{"kraken/_manifests", false, _invalidPathSubType},
		{"kraken/_manifests/tags/sometag/current", false, _invalidPathSubType},
		{"kraken/_manifests/revisions/sha256/manifestdigest", false, _invalidPathSubType},
		{"kraken/_manifests/revisions/sha256/manifestdigest/link", true, _revisions},
		{"kraken/_manifests/tags/sometag/current/link", true, _tags},
		{"kraken/_manifests/tags", true, _tags},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("matchManifestsPath %s", tc.input), func(t *testing.T) {
			require := require.New(t)
			match, subtype := matchManifestsPath(tc.input)
			require.Equal(tc.match, match)
			require.Equal(tc.subtype, subtype)
		})
	}
}

func TestManifestsPathGetDigest(t *testing.T) {

	testCases := []struct {
		input  string
		digest string
		err    error
	}{
		{"", "", InvalidRegistryPathError{_manifests, ""}},
		{"kraken/_manifests", "", InvalidRegistryPathError{_manifests, "kraken/_manifests"}},
		{"kraken/_manifests/tags/sometag/current", "", InvalidRegistryPathError{_manifests, "kraken/_manifests/tags/sometag/current"}},
		{"kraken/_manifests/sometag/link", "", InvalidRegistryPathError{_manifests, "kraken/_manifests/sometag/link"}},
		{"kraken/_manifests/tags/sometag/current/link", "", InvalidRegistryPathError{_manifests, "kraken/_manifests/tags/sometag/current/link"}},
		{"kraken/_manifests/tags/sometag/index/sha256/ff3a5c916c92643ff77519ffa742d3ec61b7f591b6b7504599d95a4a41134e28/link", "ff3a5c916c92643ff77519ffa742d3ec61b7f591b6b7504599d95a4a41134e28", nil},
		{"kraken/_manifests/revisions/sha256/ff3a5c916c92643ff77519ffa742d3ec61b7f591b6b7504599d95a4a41134e28/link", "ff3a5c916c92643ff77519ffa742d3ec61b7f591b6b7504599d95a4a41134e28", nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetManifestDigest:%s", tc.input), func(t *testing.T) {
			require := require.New(t)
			digest, err := GetManifestDigest(tc.input)
			require.Equal(tc.err, err)
			require.Equal(tc.digest, digest)
		})
	}
}

func TestManifestsPathGetTag(t *testing.T) {
	testCases := []struct {
		input     string
		tag       string
		isCurrent bool
		err       error
	}{
		{"", "", false, InvalidRegistryPathError{_manifests, ""}},
		{"kraken/_manifests", "", false, InvalidRegistryPathError{_manifests, "kraken/_manifests"}},
		{"kraken/_manifests/tags/sometag/current", "", false, InvalidRegistryPathError{_manifests, "kraken/_manifests/tags/sometag/current"}},
		{"kraken/_manifests/sometag/link", "", false, InvalidRegistryPathError{_manifests, "kraken/_manifests/sometag/link"}},
		{"kraken/_manifests/revisions/manifestdigest/link", "", false, InvalidRegistryPathError{_manifests, "kraken/_manifests/revisions/manifestdigest/link"}},
		{"kraken/_manifests/tags/sometag/sometag/index/sha256/manifestdigest/link", "", false, InvalidRegistryPathError{_manifests, "kraken/_manifests/tags/sometag/sometag/index/sha256/manifestdigest/link"}},
		{"kraken/_manifests/tags/sometag/index/sha256/manifestdigest/link", "sometag", false, nil},
		{"kraken/_manifests/tags/sometag/current/link", "sometag", true, nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetRepositoriesRepo %s", tc.input), func(t *testing.T) {
			require := require.New(t)
			tag, isCurrent, err := GetManifestTag(tc.input)
			require.Equal(tc.tag, tag)
			require.Equal(tc.isCurrent, isCurrent)
			require.Equal(tc.err, err)
		})
	}
}

func TestUploadsPathMatch(t *testing.T) {
	testCases := []struct {
		input   string
		match   bool
		subtype PathSubType
	}{
		{"", false, _invalidPathSubType},
		{"kraken/_uploads", false, _invalidPathSubType},
		{"kraken/_uploads/data", false, _invalidPathSubType},
		{"kraken/_uploads/uuid/data/extra", false, _invalidPathSubType},
		{"kraken/_uploads/uuid/startedat/extra", false, _invalidPathSubType},
		{"kraken/_uploads/uuid/hashstates", false, _invalidPathSubType},
		{"kraken/_uploads/uuid/hashstates/sha256/a", false, _invalidPathSubType},
		{"kraken/_uploads/uuid/data", true, _data},
		{"kraken/_uploads/uuid/startedat", true, _startedat},
		{"kraken/_uploads/uuid/hashstates/sha256/0", true, _hashstates},
		{"kraken/_uploads/uuid/hashstates/sha256", true, _hashstates},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("matchUploadsPath %s", tc.input), func(t *testing.T) {
			require := require.New(t)
			match, subtype := matchUploadsPath(tc.input)
			require.Equal(tc.match, match)
			require.Equal(tc.subtype, subtype)
		})
	}
}

func TestUploadsPathGetUUID(t *testing.T) {
	testCases := []struct {
		input string
		uuid  string
		err   error
	}{
		{"", "", InvalidRegistryPathError{_uploads, ""}},
		{"kraken/_uploads", "", InvalidRegistryPathError{_uploads, "kraken/_uploads"}},
		{"kraken/_uploads/data", "", InvalidRegistryPathError{_uploads, "kraken/_uploads/data"}},
		{"kraken/_uploads/uuid/data/extra", "", InvalidRegistryPathError{_uploads, "kraken/_uploads/uuid/data/extra"}},
		{"kraken/_uploads/uuid/hashstates", "", InvalidRegistryPathError{_uploads, "kraken/_uploads/uuid/hashstates"}},
		{"kraken/_uploads/uuid/hashstates/sha256/a", "", InvalidRegistryPathError{_uploads, "kraken/_uploads/uuid/hashstates/sha256/a"}},
		{"kraken/_uploads/uuid/uuid/data", "", InvalidRegistryPathError{_uploads, "kraken/_uploads/uuid/uuid/data"}},
		{"kraken/_uploads/uuid/data", "uuid", nil},
		{"kraken/_uploads/uuid/startedat", "uuid", nil},
		{"kraken/_uploads/uuid/hashstates/sha256", "uuid", nil},
		{"kraken/_uploads/uuid/hashstates/sha256/0", "uuid", nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetUploadUUID:%s", tc.input), func(t *testing.T) {
			require := require.New(t)
			uuid, err := GetUploadUUID(tc.input)
			require.Equal(tc.uuid, uuid)
			require.Equal(tc.err, err)
		})
	}
}

func TestUploadsPathGetAlgoAndOffset(t *testing.T) {
	testCases := []struct {
		input  string
		algo   string
		offset string
		err    error
	}{
		{"", "", "", InvalidRegistryPathError{_uploads, ""}},
		{"kraken/_uploads/uuid/hashstates", "", "", InvalidRegistryPathError{_uploads, "kraken/_uploads/uuid/hashstates"}},
		{"kraken/_uploads/uuid/hashstates/sha256", "", "", InvalidRegistryPathError{_uploads, "kraken/_uploads/uuid/hashstates/sha256"}},
		{"kraken/_uploads/uuid/hashstates/sha256/a", "", "", InvalidRegistryPathError{_uploads, "kraken/_uploads/uuid/hashstates/sha256/a"}},
		{"kraken/_uploads/uuid/hashstates/sha256/0", "sha256", "0", nil},
		{"kraken/_uploads/uuid/hashstates/sha256/1234", "sha256", "1234", nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetUploadAlgoAndOffset:%s", tc.input), func(t *testing.T) {
			require := require.New(t)
			algo, offset, err := GetUploadAlgoAndOffset(tc.input)
			require.Equal(tc.algo, algo)
			require.Equal(tc.offset, offset)
			require.Equal(tc.err, err)
		})
	}
}
