package bandwidth

import (
	"fmt"
	"time"

	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"
	"golang.org/x/time/rate"
)

// Config defines Limiter configuration.
type Config struct {
	EgressBitsPerSec uint64 `yaml:"egress_bits_per_sec"`

	// TokenSize defines the granularity of a token in the bucket. It is used to
	// avoid integer overflow errors that would occur if we mapped each bit to a
	// token.
	TokenSize uint64 `yaml:"token_size"`

	Disable bool `yaml:"disable"`
}

func (c Config) applyDefaults() Config {
	if c.EgressBitsPerSec == 0 {
		c.EgressBitsPerSec = 600 * memsize.Mbit
	}
	if c.TokenSize == 0 {
		c.TokenSize = memsize.Mbit
	}
	return c
}

// Limiter limits egress (and in the future, ingress) bandwidth via token-bucket
// rate limiter.
type Limiter struct {
	config Config
	egress *rate.Limiter
}

// NewLimiter creates a new Limiter.
func NewLimiter(config Config) *Limiter {
	config = config.applyDefaults()

	if config.Disable {
		log.Warn("Bandwidth limits disabled")
	} else {
		log.Infof("Setting egress bandwidth to %s/sec", memsize.BitFormat(config.EgressBitsPerSec))
	}

	tps := config.EgressBitsPerSec / config.TokenSize

	return &Limiter{
		config: config,
		egress: rate.NewLimiter(rate.Limit(tps), int(tps)),
	}
}

// ReserveEgress blocks until bandwidth for nbytes is available. Returns error
// if nbytes is larger than the maximum bandwidth.
func (l *Limiter) ReserveEgress(nbytes int64) error {
	if l.config.Disable {
		return nil
	}
	tokens := int(uint64(nbytes*8) / l.config.TokenSize)
	if tokens == 0 {
		tokens = 1
	}
	r := l.egress.ReserveN(time.Now(), tokens)
	if !r.OK() {
		return fmt.Errorf(
			"cannot reserve %s of egress bandwidth, max is %s",
			memsize.Format(uint64(nbytes)),
			memsize.BitFormat(l.config.TokenSize*uint64(l.egress.Burst())))
	}
	time.Sleep(r.Delay())
	return nil
}
