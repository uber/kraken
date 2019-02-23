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
package dockerregistry

import (
	"fmt"
	"testing"

	"github.com/uber/kraken/core"

	"github.com/stretchr/testify/require"
)

const _testDigestHex = "ff3a5c916c92643ff77519ffa742d3ec61b7f591b6b7504599d95a4a41134e28"

func TestBlobsPath(t *testing.T) {
	d := core.DigestFixture()

	result, err := GetBlobDigest(fmt.Sprintf("/v2/blobs/sha256/%s/%s/data", d.Hex()[:2], d.Hex()))
	require.NoError(t, err)
	require.Equal(t, d, result)
}

func TestBlobsPathNoMatch(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"more than 2 char digest prefix", fmt.Sprintf("/v2/blobs/sha256/1234/%s/data", _testDigestHex)},
		{"invalid path", fmt.Sprintf("/v2/blobs/sha256/1234/%s", _testDigestHex)},
		{"invalid char in digest", fmt.Sprintf("/v2/blobs/sha256/3Z/%s/data", _testDigestHex)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			_, err := GetBlobDigest(tc.input)
			expectedErr := InvalidRegistryPathError{_blobs, tc.input}
			require.Equal(expectedErr, err)
		})
	}
}

func TestRepositoriesPath(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		repo  string
	}{
		{"single namespace manifest", "/v2/repositories/kraken/_manifests", "kraken"},
		{"single namespace upload", "/v2/repositories/kraken/_uploads", "kraken"},
		{"single namespace layer", "/v2/repositories/kraken/_layers", "kraken"},
		{"multiple namespace manifest", "/v2/repositories/namespace-foo/kraken/_manifests", "namespace-foo/kraken"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			repo, err := GetRepo(tc.input)
			require.NoError(err)
			require.Equal(tc.repo, repo)
		})
	}
}

func TestRepositoriesPathNoMatch(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		repo  string
	}{
		{"empty path", "", ""},
		{"no namespace", "/v2/repositories/_manifests", ""},
		{"no repositories prefix", "/v2/repo/kraken/_manifests", ""},
		{"missing _manifests", "/v2/repositories/kraken/versions", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			repo, err := GetRepo(tc.input)
			require.Equal(tc.repo, repo)
			expectedErr := InvalidRegistryPathError{_repositories, tc.input}
			require.Equal(expectedErr, err)
		})
	}
}

func TestLayersPath(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		match   bool
		subtype PathSubType
	}{
		{"empty path", "", false, _invalidPathSubType},
		{"missing algo", "kraken/_layers/digest5678/", false, _invalidPathSubType},
		{"missing layer or link", "kraken/_layers/sha256/digest5678/", false, _invalidPathSubType},
		{"invalid char in digest", "kraken/_layers/sha256/digestZ5678/data", false, _invalidPathSubType},
		{"valid data path", "kraken/_layers/sha256/digest5678/data", true, _data},
		{"valid link path", "kraken/_layers/sha256/digest5678/link", true, _link},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			match, subtype := matchLayersPath(tc.input)
			require.Equal(tc.match, match)
			require.Equal(tc.subtype, subtype)
		})
	}
}

func TestLayersPathGetDigest(t *testing.T) {
	d := core.DigestFixture()

	testCases := []struct {
		name  string
		input string
	}{
		{"valid data path", fmt.Sprintf("kraken/_layers/sha256/%s/data", d.Hex())},
		{"valid link path", fmt.Sprintf("kraken/_layers/sha256/%s/link", d.Hex())},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := GetLayerDigest(tc.input)
			require.NoError(t, err)
			require.Equal(t, d, result)
		})
	}
}

func TestLayersPathGetDigestNoMatch(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"missing sha256", fmt.Sprintf("kraken/_layers/%s/", _testDigestHex)},
		{"missing data or link", fmt.Sprintf("kraken/_layers/sha256/%s/", _testDigestHex)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			_, err := GetLayerDigest(tc.input)
			expectedErr := InvalidRegistryPathError{_layers, tc.input}
			require.Equal(expectedErr, err)
		})
	}
}

func TestManifestsPathMatch(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		match   bool
		subtype PathSubType
	}{
		{"empty", "", false, _invalidPathSubType},
		{"incomplete", "kraken/_manifests", false, _invalidPathSubType},
		{"missing link for current path", "kraken/_manifests/tags/sometag/current", false, _invalidPathSubType},
		{"missing link for manifest path", "kraken/_manifests/revisions/sha256/manifestdigest", false, _invalidPathSubType},
		{"valid manifest link", "kraken/_manifests/revisions/sha256/manifestdigest/link", true, _revisions},
		{"valid tag link", "kraken/_manifests/tags/sometag/current/link", true, _tags},
		{"valid tags", "kraken/_manifests/tags", true, _tags},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			match, subtype := matchManifestsPath(tc.input)
			require.Equal(tc.match, match)
			require.Equal(tc.subtype, subtype)
		})
	}
}

func TestManifestsPathGetDigest(t *testing.T) {
	d := core.DigestFixture()

	testCases := []struct {
		name  string
		input string
	}{
		{"valid tag digest", fmt.Sprintf("kraken/_manifests/tags/sometag/index/sha256/%s/link", d.Hex())},
		{"valid revision digest", fmt.Sprintf("kraken/_manifests/revisions/sha256/%s/link", d.Hex())},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := GetManifestDigest(tc.input)
			require.NoError(t, err)
			require.Equal(t, d, result)
		})
	}
}

func TestManifestsPathGetDigestNoMatch(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"incomplete", "kraken/_manifests"},
		{"missing link", "kraken/_manifests/tags/sometag/current"},
		{"no digest in tag", "kraken/_manifests/sometag/link"},
		{"no digest in current", "kraken/_manifests/tags/sometag/current/link"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			_, err := GetManifestDigest(tc.input)
			expectedErr := InvalidRegistryPathError{_manifests, tc.input}
			require.Equal(expectedErr, err)
		})
	}
}

func TestManifestsPathGetTag(t *testing.T) {
	testCases := []struct {
		name      string
		input     string
		tag       string
		isCurrent bool
	}{
		{"valid tag index", "kraken/_manifests/tags/sometag/index/sha256/manifestdigest/link", "sometag", false},
		{"valid tag current", "kraken/_manifests/tags/sometag/current/link", "sometag", true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetRepositoriesRepo %s", tc.name), func(t *testing.T) {
			require := require.New(t)
			tag, isCurrent, err := GetManifestTag(tc.input)
			require.NoError(err)
			require.Equal(tc.tag, tag)
			require.Equal(tc.isCurrent, isCurrent)
		})
	}
}

func TestManifestsPathGetTagNoMatch(t *testing.T) {
	testCases := []struct {
		name      string
		input     string
		tag       string
		isCurrent bool
	}{
		{"empty", "", "", false},
		{"incomplete", "kraken/_manifests", "", false},
		{"missing link", "kraken/_manifests/tags/sometag/current", "", false},
		{"missing current", "kraken/_manifests/sometag/link", "", false},
		{"missing algo", "kraken/_manifests/revisions/manifestdigest/link", "", false},
		{"more than one tag in path", "kraken/_manifests/tags/sometag/sometag/index/sha256/manifestdigest/link", "", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			tag, isCurrent, err := GetManifestTag(tc.input)
			require.Equal(tc.tag, tag)
			require.Equal(tc.isCurrent, isCurrent)
			expectedErr := InvalidRegistryPathError{_manifests, tc.input}
			require.Equal(expectedErr, err)
		})
	}
}

func TestUploadsPathMatch(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		match   bool
		subtype PathSubType
	}{
		{"empty", "", false, _invalidPathSubType},
		{"incomplete", "kraken/_uploads", false, _invalidPathSubType},
		{"missing uuid", "kraken/_uploads/data", false, _invalidPathSubType},
		{"extra suffix after data", "kraken/_uploads/uuid/data/extra", false, _invalidPathSubType},
		{"extra suffix after startedat", "kraken/_uploads/uuid/startedat/extra", false, _invalidPathSubType},
		{"missing digest", "kraken/_uploads/uuid/hashstates", false, _invalidPathSubType},
		{"invalid offset", "kraken/_uploads/uuid/hashstates/sha256/a", false, _invalidPathSubType},
		{"valid data", "kraken/_uploads/uuid/data", true, _data},
		{"valid startedat", "kraken/_uploads/uuid/startedat", true, _startedat},
		{"valid hashstates with offset", "kraken/_uploads/uuid/hashstates/sha256/0", true, _hashstates},
		{"valid hashstate without offset", "kraken/_uploads/uuid/hashstates/sha256", true, _hashstates},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			match, subtype := matchUploadsPath(tc.input)
			require.Equal(tc.match, match)
			require.Equal(tc.subtype, subtype)
		})
	}
}

func TestUploadsPathGetUUID(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		uuid  string
	}{
		{"valid data", "kraken/_uploads/uuid/data", "uuid"},
		{"valid startedat", "kraken/_uploads/uuid/startedat", "uuid"},
		{"valid hashstates", "kraken/_uploads/uuid/hashstates/sha256", "uuid"},
		{"valid hashstates with offset", "kraken/_uploads/uuid/hashstates/sha256/0", "uuid"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			uuid, err := GetUploadUUID(tc.input)
			require.NoError(err)
			require.Equal(tc.uuid, uuid)
		})
	}
}

func TestUploadsPathGetUUIDNoMatch(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		uuid  string
	}{
		{"empty", "", ""},
		{"incomplete", "kraken/_uploads", ""},
		{"missing uuid", "kraken/_uploads/data", ""},
		{"extra suffix after data", "kraken/_uploads/uuid/data/extra", ""},
		{"missing algo", "kraken/_uploads/uuid/hashstates", ""},
		{"invalid offset", "kraken/_uploads/uuid/hashstates/sha256/a", ""},
		{"multiple uuid", "kraken/_uploads/uuid/uuid/data", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			uuid, err := GetUploadUUID(tc.input)
			require.Equal(tc.uuid, uuid)
			expectedErr := InvalidRegistryPathError{_uploads, tc.input}
			require.Equal(expectedErr, err)
		})
	}
}

func TestUploadsPathGetAlgoAndOffset(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		algo   string
		offset string
	}{
		{"offest 0", "kraken/_uploads/uuid/hashstates/sha256/0", "sha256", "0"},
		{"offest 1234", "kraken/_uploads/uuid/hashstates/sha256/1234", "sha256", "1234"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			algo, offset, err := GetUploadAlgoAndOffset(tc.input)
			require.NoError(err)
			require.Equal(tc.algo, algo)
			require.Equal(tc.offset, offset)
		})
	}
}

func TestUploadsPathGetAlgoAndOffsetNoMatch(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		algo   string
		offset string
	}{
		{"empty", "", "", ""},
		{"missing algo", "kraken/_uploads/uuid/hashstates", "", ""},
		{"missing offset", "kraken/_uploads/uuid/hashstates/sha256", "", ""},
		{"invalid offset", "kraken/_uploads/uuid/hashstates/sha256/a", "", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			algo, offset, err := GetUploadAlgoAndOffset(tc.input)
			require.Equal(tc.algo, algo)
			require.Equal(tc.offset, offset)
			expectedErr := InvalidRegistryPathError{_uploads, tc.input}
			require.Equal(expectedErr, err)
		})
	}
}
