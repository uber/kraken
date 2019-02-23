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
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	_testStr     = "test"
	_expectedHex = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
)

func TestNewDigester(t *testing.T) {
	require := require.New(t)

	d := NewDigester()

	hexDigest := d.Digest().Hex()
	require.NoError(ValidateSHA256(hexDigest))
}

func TestFromBytes(t *testing.T) {
	require := require.New(t)

	d := NewDigester()
	d.FromBytes([]byte(_testStr))

	hexDigest := d.Digest().Hex()
	require.NoError(ValidateSHA256(hexDigest))
	require.Equal(_expectedHex, hexDigest)
}

func TestFromReader(t *testing.T) {
	require := require.New(t)

	d := NewDigester()
	r := strings.NewReader(_testStr)
	d.FromReader(r)

	hexDigest := d.Digest().Hex()
	require.NoError(ValidateSHA256(hexDigest))
	require.Equal(_expectedHex, hexDigest)
}

func TestTeeReader(t *testing.T) {
	require := require.New(t)

	d := NewDigester()

	r := bytes.NewBufferString(_testStr)
	w := &bytes.Buffer{}
	tr := d.Tee(r)

	_, err := io.Copy(w, tr)
	require.NoError(err)
	b, err := ioutil.ReadAll(w)
	require.NoError(err)
	require.Equal(_testStr, string(b))

	hexDigest := d.Digest().Hex()
	require.NoError(ValidateSHA256(hexDigest))
	require.Equal(_expectedHex, hexDigest)
}
