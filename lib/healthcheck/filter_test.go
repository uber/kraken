package healthcheck

import (
	"context"
	"errors"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/mocks/lib/healthcheck"
	"code.uber.internal/infra/kraken/utils/stringset"

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

	f := NewFilter(Config{Fails: 1, Passes: 1}, checker)

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

	f := NewFilter(Config{Fails: 1, Passes: 1, Timeout: time.Second}, checker)

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

	f := NewFilter(Config{Fails: 1, Passes: 1}, checker)

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

	f := NewFilter(Config{Fails: 2, Passes: 2}, checker)

	checker.EXPECT().Check(gomock.Any(), x).Return(errors.New("some error")).Times(2)
	checker.EXPECT().Check(gomock.Any(), y).Return(errors.New("some error")).Times(2)

	// Even though health checks are failing, since Fails=2, it takes two Runs
	// for the unhealthy addrs to be filtered out.
	require.Equal(stringset.New(x, y), f.Run(stringset.New(x, y)))
	require.Empty(f.Run(stringset.New(x, y)))
}
