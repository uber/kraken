package dispatch

import (
	"math"
	"time"

	"code.uber.internal/infra/kraken/lib/torrent/scheduler/dispatch/piecerequest"
	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/timeutil"
)

// Config defines the configuration for piece dispatch.
type Config struct {

	// PieceRequestMinTimeout is the minimum timeout for all piece requests, regardless of
	// size.
	PieceRequestMinTimeout time.Duration `yaml:"piece_request_min_timeout"`

	// PieceRequestTimeoutPerMb is the duration that will be added to piece request
	// timeouts based on the piece size (in megabytes).
	PieceRequestTimeoutPerMb time.Duration `yaml:"piece_request_timeout_per_mb"`

	// PieceRequestPolicy is the policy that is used to decide which pieces to request
	// from a peer.
	PieceRequestPolicy string `yaml:"piece_request_policy"`

	// PipelineLimit limits the total number of requests can be sent to a peer
	// at the same time.
	PipelineLimit int `yaml:"pipeline_limit"`

	// EndgameThreshold is the number pieces required to complete the torrent
	// before the torrent enters "endgame", where we start overloading piece
	// requests to multiple peers.
	EndgameThreshold int `yaml:"endgame_threshold"`

	DisableEndgame bool `yaml:"disable_endgame"`
}

func (c Config) applyDefaults() Config {
	if c.PieceRequestPolicy == "" {
		c.PieceRequestPolicy = piecerequest.DefaultPolicy
	}
	if c.PieceRequestMinTimeout == 0 {
		c.PieceRequestMinTimeout = 4 * time.Second
	}
	if c.PieceRequestTimeoutPerMb == 0 {
		c.PieceRequestTimeoutPerMb = 4 * time.Second
	}
	if c.PipelineLimit == 0 {
		c.PipelineLimit = 3
	}
	if c.EndgameThreshold == 0 {
		c.EndgameThreshold = c.PipelineLimit
	}
	return c
}

func (c Config) calcPieceRequestTimeout(maxPieceLength int64) time.Duration {
	n := float64(c.PieceRequestTimeoutPerMb) * float64(maxPieceLength) / float64(memsize.MB)
	d := time.Duration(math.Ceil(n))
	return timeutil.MaxDuration(d, c.PieceRequestMinTimeout)
}
