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
package healthcheck

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/mocks/lib/healthcheck"
	"github.com/uber/kraken/utils/stringset"
)

func TestPassiveResolve(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	filter := mockhealthcheck.NewMockPassiveFilter(ctrl)

	x := "x:80"
	y := "y:80"

	p := NewPassive(hostlist.Fixture(x, y), filter)

	filter.EXPECT().Run(stringset.New(x, y)).Return(stringset.New(x))

	require.Equal(stringset.New(x), p.Resolve())
}

func TestPassiveResolveIgnoresAllUnhealthy(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	filter := mockhealthcheck.NewMockPassiveFilter(ctrl)

	x := "x:80"
	y := "y:80"

	p := NewPassive(hostlist.Fixture(x, y), filter)

	filter.EXPECT().Run(stringset.New(x, y)).Return(stringset.New())

	require.Equal(stringset.New(x, y), p.Resolve())
}

func TestPassiveFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	filter := mockhealthcheck.NewMockPassiveFilter(ctrl)

	x := "x:80"
	y := "y:80"

	p := NewPassive(hostlist.Fixture(x, y), filter)

	filter.EXPECT().Failed(x)

	p.Failed(x)
}
