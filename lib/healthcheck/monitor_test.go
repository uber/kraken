package healthcheck_test

import (
	"testing"
	"time"

	. "code.uber.internal/infra/kraken/lib/healthcheck"
	"code.uber.internal/infra/kraken/mocks/lib/healthcheck"
	"code.uber.internal/infra/kraken/utils/stringset"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestMonitor(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	filter := mockhealthcheck.NewMockFilter(ctrl)

	filter.EXPECT().Init().Return(nil)
	filter.EXPECT().Run().Times(2)

	m, err := NewMonitor(MonitorConfig{Interval: 450 * time.Millisecond}, filter)
	require.NoError(err)
	defer m.Stop()

	time.Sleep(time.Second)

	s := stringset.New("x:80", "y:80")

	filter.EXPECT().GetHealthy().Return(s)

	require.Equal(s, m.GetHealthy())
}
