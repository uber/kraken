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

func TestPrometheusConfigDefaults(t *testing.T) {
	c := PrometheusConfig{ListenAddress: ":9091"}.applyDefaults()
	require.Equal(t, "/metrics", c.HandlerPath)
	require.Equal(t, "kraken", c.Prefix)
	require.Equal(t, _defaultPrometheusTimerBuckets, c.TimerBuckets)
	require.NoError(t, c.validate())

	c = PrometheusConfig{TimerBuckets: []float64{1, 2}}.applyDefaults()
	require.Equal(t, []float64{1, 2}, c.TimerBuckets)
}

func TestPrometheusConfigValidation(t *testing.T) {
	tests := []struct {
		desc   string
		config PrometheusConfig
		errMsg string
	}{
		{"missing listen address", PrometheusConfig{}, "listen_address required"},
		{
			"bad handler path",
			PrometheusConfig{ListenAddress: "127.0.0.1:0", HandlerPath: "metrics"},
			"must start with /",
		},
		{
			"unsorted buckets",
			PrometheusConfig{ListenAddress: "127.0.0.1:0", TimerBuckets: []float64{1, 0.5}},
			"must be sorted",
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			err := test.config.applyDefaults().validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), test.errMsg)
		})
	}
}
