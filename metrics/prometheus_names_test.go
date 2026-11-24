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
package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSanitizePrometheusName(t *testing.T) {
	tests := map[string]string{
		"valid_name":     "valid_name",
		"with.dots":      "with_dots",
		"9starts":        "_9starts",
		"a9":             "a9",
		"unicode_\u00e9": "unicode__",
		"":               "_",
	}
	for in, want := range tests {
		require.Equal(t, want, sanitizePrometheusName(in), "input %q", in)
	}
}

func TestSeriesKey(t *testing.T) {
	require := require.New(t)

	// Label order does not matter.
	require.Equal(
		seriesKey("m", map[string]string{"a": "1", "b": "2"}),
		seriesKey("m", map[string]string{"b": "2", "a": "1"}))

	// Name, label names and label values all distinguish series.
	keys := map[string]bool{
		seriesKey("m", nil):                                  true,
		seriesKey("n", nil):                                  true,
		seriesKey("m", map[string]string{"a": "1"}):          true,
		seriesKey("m", map[string]string{"a": "2"}):          true,
		seriesKey("m", map[string]string{"b": "1"}):          true,
		seriesKey("m", map[string]string{"a": "1=b"}):        true,
		seriesKey("m", map[string]string{"a": "1", "b": ""}): true,
	}
	require.Len(keys, 7)
}
