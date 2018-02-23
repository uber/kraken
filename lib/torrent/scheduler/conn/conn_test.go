package conn

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
	bitsPerSec := memsize.Kbit
	expectedDur := time.Duration(8*size/bitsPerSec) * time.Second

	info, cleanup := storage.TorrentInfoFixture(pieceLength*4, pieceLength)
	defer cleanup()

	local, remote, cleanup := PipeFixture(ConfigFixture(), info)
	defer cleanup()

	complete := make(chan bool)
	go func() {
		var n int
		for range remote.Receiver() {
			n++
			if n == numPieces {
				complete <- true
				return
			}
		}
		complete <- false
	}()

	local.SetEgressBandwidthLimit(bitsPerSec)

	start := time.Now()
	for i := 0; i < numPieces; i++ {
		go func() {
			pr := storage.NewPieceReaderBuffer(randutil.Text(pieceLength))
			msg := NewPiecePayloadMessage(0, pr)
			require.NoError(local.Send(msg))
		}()
	}

	if <-complete {
		stop := time.Now()
		require.WithinDuration(start.Add(expectedDur), stop, 250*time.Millisecond)
	} else {
		require.FailNow("Receiver closed early")
	}
}
