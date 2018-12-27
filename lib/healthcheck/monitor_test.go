package healthcheck

import (
	"testing"
	"time"

	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/mocks/lib/healthcheck"
	"github.com/uber/kraken/utils/stringset"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestActiveMonitor(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	x := "x:80"
	y := "y:80"

	filter := mockhealthcheck.NewMockFilter(ctrl)

	filter.EXPECT().Run(stringset.New(x, y)).Return(stringset.New(x))

	m := NewMonitor(
		MonitorConfig{Interval: time.Second},
		hostlist.Fixture(x, y),
		filter)
	defer m.Stop()

	all := stringset.New(x, y)
	resolvedHealthy, resolvedAll := m.Resolve()
	require.Equal(all, resolvedHealthy)
	require.Equal(all, resolvedAll)

	time.Sleep(1250 * time.Millisecond)

	resolvedHealthy, resolvedAll = m.Resolve()
	require.Equal(stringset.New(x), resolvedHealthy)
	require.Equal(all, resolvedAll)
}
