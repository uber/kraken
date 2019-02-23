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
package core

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateSHA256(t *testing.T) {
	tests := []struct {
		desc  string
		input string
		err   bool
	}{
		{"valid", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", false},
		{"too short", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85", true},
		{"too long", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8556", true},
		{"invalid hex", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", true},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			err := ValidateSHA256(test.input)
			if test.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewSHA256DigestFromHex(t *testing.T) {
	require := require.New(t)

	d, err := NewSHA256DigestFromHex("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	require.NoError(err)
	require.Equal("sha256", d.Algo())
	require.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", d.Hex())
	require.Equal("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", d.String())
	v, err := d.Value()
	require.NoError(err)
	require.Equal([]byte("\"sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\""), v)
	require.Equal("e3b0", d.ShardID())
}

func TestNewSHA256DigestFromHexError(t *testing.T) {
	_, err := NewSHA256DigestFromHex("invalid")
	require.Error(t, err)
}

func TestParseSHA256Digest(t *testing.T) {
	require := require.New(t)

	d, err := ParseSHA256Digest("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	require.NoError(err)
	require.Equal("sha256", d.Algo())
	require.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", d.Hex())
	require.Equal("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", d.String())
	v, err := d.Value()
	require.NoError(err)
	require.Equal([]byte("\"sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\""), v)
	require.Equal("e3b0", d.ShardID())
}

func TestParseSHA256DigestErrors(t *testing.T) {
	tests := []struct {
		desc  string
		input string
	}{
		{"empty", ""},
		{"extra part", "sha256:sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"no algo", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"wrong algo", "sha1:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"invalid hex", "sha256:invalid"},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			_, err := ParseSHA256Digest(test.input)
			require.Error(t, err)
		})
	}
}

func TestDigestStringConversion(t *testing.T) {
	d := DigestFixture()
	result, err := ParseSHA256Digest(d.String())
	require.NoError(t, err)
	require.Equal(t, d, result)
}

func TestDigestMarshalJSON(t *testing.T) {
	require := require.New(t)
	digest := DigestFixture()

	b, err := json.Marshal(digest)
	require.NoError(err)
	require.Equal(string(b), fmt.Sprintf("%q", digest))

	var result Digest
	require.NoError(json.Unmarshal(b, &result))
	require.Equal(digest, result)
}

func TestDigestUnmarshalJSONErrors(t *testing.T) {
	tests := []struct {
		desc  string
		input string
	}{
		{"empty", ""},
		{"invalid", "\"sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"no algo", "\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\""},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			var d Digest
			require.Error(t, d.UnmarshalJSON([]byte(test.input)))
		})
	}
}

func TestDigestListValue(t *testing.T) {
	require := require.New(t)
	digests := DigestList{DigestFixture(), DigestFixture(), DigestFixture()}
	v, err := digests.Value()
	require.NoError(err)
	expected := fmt.Sprintf("[%q,%q,%q]", digests[0], digests[1], digests[2])
	require.Equal(expected, fmt.Sprintf("%s", v))

	var result DigestList
	require.NoError(result.Scan([]byte(expected)))
	require.Equal(digests, result)
}
