package healthcheck

import (
	"testing"
	"time"

	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/mocks/lib/healthcheck"
	"code.uber.internal/infra/kraken/utils/stringset"

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

	require.Equal(stringset.New(x, y), m.Resolve())

	time.Sleep(1250 * time.Millisecond)

	require.Equal(stringset.New(x), m.Resolve())
}
