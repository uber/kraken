package healthcheck

import (
	"context"
	"errors"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/mocks/lib/healthcheck"
	"code.uber.internal/infra/kraken/mocks/lib/hostlist"
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

	f := NewFilter(
		FilterConfig{Fails: 1, Passes: 1, Timeout: time.Second},
		checker,
		hostlist.Fixture(x, y))

	require.Empty(f.GetHealthy())

	checker.EXPECT().Check(gomock.Any(), x).Return(nil)
	checker.EXPECT().Check(gomock.Any(), y).Return(nil)

	require.NoError(f.Run())

	require.Equal(stringset.New(x, y), f.GetHealthy())

	checker.EXPECT().Check(gomock.Any(), x).Return(errors.New("some error"))
	checker.EXPECT().Check(gomock.Any(), y).Return(errors.New("some error"))

	require.NoError(f.Run())

	require.Empty(f.GetHealthy())
}

func TestFilterCheckTimeout(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	checker := mockhealthcheck.NewMockChecker(ctrl)

	x := "x:80"
	y := "y:80"

	f := NewFilter(
		FilterConfig{Fails: 1, Passes: 1, Timeout: time.Second},
		checker,
		hostlist.Fixture(x, y))

	require.Empty(f.GetHealthy())

	checker.EXPECT().Check(gomock.Any(), x).Return(nil)
	checker.EXPECT().Check(gomock.Any(), y).DoAndReturn(func(context.Context, string) error {
		time.Sleep(2 * time.Second)
		return nil
	})

	require.NoError(f.Run())

	require.Equal(stringset.New(x), f.GetHealthy())
}

func TestFilterSingleHostAlwaysHealthy(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	checker := mockhealthcheck.NewMockChecker(ctrl)

	x := "x:80"

	f := NewFilter(
		FilterConfig{Fails: 1, Passes: 1, Timeout: time.Second},
		checker,
		hostlist.Fixture(x))

	require.Empty(f.GetHealthy())

	// No health checks actually run since only single host is used.
	require.NoError(f.Run())

	require.Equal(stringset.New(x), f.GetHealthy())
}

func TestFilterHostChanges(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	checker := mockhealthcheck.NewMockChecker(ctrl)
	hosts := mockhostlist.NewMockList(ctrl)

	x := "x:80"
	y := "y:80"
	z := "z:80"

	f := NewFilter(
		FilterConfig{Fails: 1, Passes: 1, Timeout: time.Second},
		checker,
		hosts)

	require.Empty(f.GetHealthy())

	hosts.EXPECT().ResolveNonLocal().Return(stringset.New(x, y), nil)
	checker.EXPECT().Check(gomock.Any(), x).Return(nil)
	checker.EXPECT().Check(gomock.Any(), y).Return(nil)

	require.NoError(f.Run())

	require.Equal(stringset.New(x, y), f.GetHealthy())

	// x is removed and z is added.
	hosts.EXPECT().ResolveNonLocal().Return(stringset.New(y, z), nil)
	checker.EXPECT().Check(gomock.Any(), y).Return(nil)
	checker.EXPECT().Check(gomock.Any(), z).Return(nil)

	require.NoError(f.Run())

	require.Equal(stringset.New(y, z), f.GetHealthy())
}

func TestFilterInit(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	checker := mockhealthcheck.NewMockChecker(ctrl)

	x := "x:80"
	y := "y:80"

	f := NewFilter(
		FilterConfig{Fails: 1, Passes: 1, Timeout: time.Second},
		checker,
		hostlist.Fixture(x, y))

	require.Empty(f.GetHealthy())

	require.NoError(f.Init())

	require.Equal(stringset.New(x, y), f.GetHealthy())
}
