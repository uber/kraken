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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewInfoHashFromHex(t *testing.T) {
	require := require.New(t)

	d, err := NewInfoHashFromHex("e3b0c44298fc1c149afbf4c8996fb92427ae41e4")
	require.NoError(err)
	require.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4", d.Hex())
	require.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4", d.String())
}

func TestNewInfoHashFromHexErrors(t *testing.T) {
	tests := []struct {
		desc  string
		input string
	}{
		{"empty", ""},
		{"too long", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"invalid hex", "x3b0c44298fc1c149afbf4c8996fb92427ae41e4"},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			_, err := NewInfoHashFromHex(test.input)
			require.Error(t, err)
		})
	}
}
