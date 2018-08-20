package bandwidth

import (
	"fmt"
	"time"

	"code.uber.internal/infra/kraken/utils/memsize"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Config defines Limiter configuration.
type Config struct {
	EgressBitsPerSec  uint64 `yaml:"egress_bits_per_sec"`
	IngressBitsPerSec uint64 `yaml:"ingress_bits_per_sec"`

	// TokenSize defines the granularity of a token in the bucket. It is used to
	// avoid integer overflow errors that would occur if we mapped each bit to a
	// token.
	TokenSize uint64 `yaml:"token_size"`

	Disable bool `yaml:"disable"`
}

func (c Config) applyDefaults() Config {
	if c.EgressBitsPerSec == 0 {
		c.EgressBitsPerSec = 200 * memsize.Mbit
	}
	if c.IngressBitsPerSec == 0 {
		c.IngressBitsPerSec = 300 * memsize.Mbit
	}
	if c.TokenSize == 0 {
		c.TokenSize = memsize.Mbit
	}
	return c
}

// Limiter limits egress and ingress bandwidth via token-bucket rate limiter.
type Limiter struct {
	config  Config
	egress  *rate.Limiter
	ingress *rate.Limiter
}

// NewLimiter creates a new Limiter.
func NewLimiter(config Config, logger *zap.SugaredLogger) *Limiter {
	config = config.applyDefaults()

	if config.Disable {
		logger.Warn("Bandwidth limits disabled")
	} else {
		logger.Infof("Setting egress bandwidth to %s/sec", memsize.BitFormat(config.EgressBitsPerSec))
		logger.Infof("Setting ingress bandwidth to %s/sec", memsize.BitFormat(config.IngressBitsPerSec))
	}

	etps := config.EgressBitsPerSec / config.TokenSize
	itps := config.IngressBitsPerSec / config.TokenSize

	return &Limiter{
		config:  config,
		egress:  rate.NewLimiter(rate.Limit(etps), int(etps)),
		ingress: rate.NewLimiter(rate.Limit(itps), int(itps)),
	}
}

func (l *Limiter) reserve(rl *rate.Limiter, nbytes int64) error {
	if l.config.Disable {
		return nil
	}
	tokens := int(uint64(nbytes*8) / l.config.TokenSize)
	if tokens == 0 {
		tokens = 1
	}
	r := rl.ReserveN(time.Now(), tokens)
	if !r.OK() {
		return fmt.Errorf(
			"cannot reserve %s of bandwidth, max is %s",
			memsize.Format(uint64(nbytes)),
			memsize.BitFormat(l.config.TokenSize*uint64(rl.Burst())))
	}
	time.Sleep(r.Delay())
	return nil
}

// ReserveEgress blocks until egress bandwidth for nbytes is available.
// Returns error if nbytes is larger than the maximum egress bandwidth.
func (l *Limiter) ReserveEgress(nbytes int64) error {
	return l.reserve(l.egress, nbytes)
}

// ReserveIngress blocks until ingress bandwidth for nbytes is available.
// Returns error if nbytes is larger than the maximum ingress bandwidth.
func (l *Limiter) ReserveIngress(nbytes int64) error {
	return l.reserve(l.ingress, nbytes)
}
