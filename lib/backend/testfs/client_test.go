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
package testfs

import (
	"errors"
	"testing"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/lib/backend/namepath"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestClientFactory(t *testing.T) {
	require := require.New(t)

	config := Config{
		Addr:     "test",
		NamePath: namepath.Identity,
	}
	f := factory{}
	_, err := f.Create(config, nil, tally.NoopScope, zap.NewNop().Sugar())
	require.NoError(err)
}

func TestClientFactoryConfigError(t *testing.T) {
	tests := []struct {
		give       string
		giveConfig interface{}
		wantErr    string
	}{
		{
			give:       "unmarshalable config",
			giveConfig: []string{"not", "a", "mapping"},
			wantErr:    "unmarshal testfs config",
		},
		{
			give:       "marshal failure",
			giveConfig: failingConfig{},
			wantErr:    "marshal testfs config",
		},
	}

	f := factory{}

	for _, tt := range tests {
		t.Run(tt.give, func(t *testing.T) {
			_, err := f.Create(tt.giveConfig, nil, tally.NoopScope, zap.NewNop().Sugar())
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
			require.NotNil(t, errors.Unwrap(err))
		})
	}
}

// failingConfig makes yaml.Marshal return an error through the Marshaler
// interface. Unsupported types (channels, funcs) cannot be used here: yaml.v2
// panics on those instead of returning an error.
type failingConfig struct{}

func (failingConfig) MarshalYAML() (interface{}, error) {
	return nil, errors.New("marshal failure")
}
