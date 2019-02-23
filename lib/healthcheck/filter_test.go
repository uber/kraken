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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/uber/kraken/mocks/lib/healthcheck"
	"github.com/uber/kraken/utils/stringset"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestFilterCheckErrors(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	checker := mockhealthcheck.NewMockChecker(ctrl)

	x := "x:80"
	y := "y:80"

	f := NewFilter(FilterConfig{Fails: 1, Passes: 1}, checker)

	checker.EXPECT().Check(gomock.Any(), x).Return(nil)
	checker.EXPECT().Check(gomock.Any(), y).Return(nil)

	require.Equal(stringset.New(x, y), f.Run(stringset.New(x, y)))

	checker.EXPECT().Check(gomock.Any(), x).Return(errors.New("some error"))
	checker.EXPECT().Check(gomock.Any(), y).Return(errors.New("some error"))

	require.Empty(f.Run(stringset.New(x, y)))
}

func TestFilterCheckTimeout(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	checker := mockhealthcheck.NewMockChecker(ctrl)

	x := "x:80"
	y := "y:80"

	f := NewFilter(FilterConfig{Fails: 1, Passes: 1, Timeout: time.Second}, checker)

	checker.EXPECT().Check(gomock.Any(), x).Return(nil)
	checker.EXPECT().Check(gomock.Any(), y).DoAndReturn(func(context.Context, string) error {
		time.Sleep(2 * time.Second)
		return nil
	})

	require.Equal(stringset.New(x), f.Run(stringset.New(x, y)))
}

func TestFilterSingleHostAlwaysHealthy(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	checker := mockhealthcheck.NewMockChecker(ctrl)

	x := "x:80"

	f := NewFilter(FilterConfig{Fails: 1, Passes: 1}, checker)

	// No health checks actually run since only single host is used.
	require.Equal(stringset.New(x), f.Run(stringset.New(x)))
}

func TestFilterNewHostsStartAsHealthy(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	checker := mockhealthcheck.NewMockChecker(ctrl)

	x := "x:80"
	y := "y:80"

	f := NewFilter(FilterConfig{Fails: 2, Passes: 2}, checker)

	checker.EXPECT().Check(gomock.Any(), x).Return(errors.New("some error")).Times(2)
	checker.EXPECT().Check(gomock.Any(), y).Return(errors.New("some error")).Times(2)

	// Even though health checks are failing, since Fails=2, it takes two Runs
	// for the unhealthy addrs to be filtered out.
	require.Equal(stringset.New(x, y), f.Run(stringset.New(x, y)))
	require.Empty(f.Run(stringset.New(x, y)))
}
