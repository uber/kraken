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
package backend_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/lib/backend"
)

func TestGetFactory(t *testing.T) {
	name := t.Name()
	factory := &struct{ backend.ClientFactory }{}
	backend.Register(name, factory)

	got, err := backend.GetFactory(name)
	require.NoError(t, err)
	require.Same(t, factory, got)

	got, err = backend.GetFactory(name + "-missing")
	require.EqualError(t, err, "no backend client defined with name "+name+"-missing")
	require.Nil(t, got)
}
