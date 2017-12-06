package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/randutil"
)

func TestConnSetEgressBandwidthThrottlesPieceSending(t *testing.T) {
	require := require.New(t)

	size := 4 * memsize.KB
	numPieces := 256
	pieceLength := size / uint64(numPieces)
	bytesPerSec := memsize.KB
	expectedDur := time.Duration(size/bytesPerSec) * time.Second

	config := connConfigFixture()

	torrent, cleanup := storage.TorrentFixture(pieceLength*4, pieceLength)
	defer cleanup()

	c1, c2, cleanup := connFixture(config, torrent)
	defer cleanup()

	complete := make(chan bool)
	go func() {
		var n int
		for range c2.Receiver() {
			n++
			if n == numPieces {
				complete <- true
				return
			}
		}
		complete <- false
	}()

	c1.SetEgressBandwidthLimit(bytesPerSec)

	start := time.Now()
	for i := 0; i < numPieces; i++ {
		go func() {
			msg := newPiecePayloadMessage(0, randutil.Text(pieceLength))
			require.NoError(c1.Send(msg))
		}()
	}

	if <-complete {
		stop := time.Now()
		require.WithinDuration(start.Add(expectedDur), stop, 250*time.Millisecond)
	} else {
		require.FailNow("Receiver closed early")
	}
}
