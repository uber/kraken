package scheduler

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/randutil"
)

func TestSetEgressBandwidthThrottlesPieceSending(t *testing.T) {
	require := require.New(t)

	size := 4 * memsize.KB
	numPieces := 256
	pieceLength := int(size / uint64(numPieces))
	bytesPerSec := memsize.KB
	expectedDur := time.Duration(size/bytesPerSec) * time.Second

	c, cleanup := genTestConn(t, genConnConfig(), pieceLength)
	defer cleanup()

	c.SetEgressBandwidthLimit(bytesPerSec)

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < numPieces; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := newPiecePayloadMessage(0, randutil.Text(pieceLength))
			require.NoError(c.Send(msg))
		}()
	}
	wg.Wait()
	stop := time.Now()

	// FIXME(codyg): If this test is prone to flakiness, run the test body a
	// few times and remove any outlier outcomes.
	require.WithinDuration(start.Add(expectedDur), stop, 250*time.Millisecond)
}
