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
package blobserver

import (
	"net/http"
	"testing"

	"github.com/uber/kraken/utils/handler"
	"github.com/stretchr/testify/require"
)

func TestParseContentRangeHeaderBadRequests(t *testing.T) {
	tests := []struct {
		description string
		value       string
	}{
		{"empty value", ""},
		{"invalid format", "blah"},
		{"invalid start", "blah-5"},
		{"invalid end", "5-blah"},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require := require.New(t)

			h := http.Header{}
			h.Add("Content-Range", test.value)
			start, end, err := parseContentRange(h)
			require.Error(err)
			require.Equal(http.StatusBadRequest, err.(*handler.Error).GetStatus())
			require.Equal(int64(0), start)
			require.Equal(int64(0), end)
		})
	}
}
