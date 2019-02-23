// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package bandwidth

import (
	"errors"
	"fmt"
	"time"

	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/memsize"

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

	Enable bool `yaml:"enable"`
}

func (c Config) applyDefaults() Config {
	if c.TokenSize == 0 {
		c.TokenSize = 8 * memsize.Mbit
	}
	return c
}

// Limiter limits egress and ingress bandwidth via token-bucket rate limiter.
type Limiter struct {
	config  Config
	egress  *rate.Limiter
	ingress *rate.Limiter
	logger  *zap.SugaredLogger
}

// Option allows setting optional parameters in Limiter.
type Option func(*Limiter)

// WithLogger configures a Limiter with a custom logger.
func WithLogger(logger *zap.SugaredLogger) Option {
	return func(l *Limiter) { l.logger = logger }
}

// NewLimiter creates a new Limiter.
func NewLimiter(config Config, opts ...Option) (*Limiter, error) {
	config = config.applyDefaults()

	l := &Limiter{
		config: config,
		logger: log.Default(),
	}
	for _, opt := range opts {
		opt(l)
	}

	if !config.Enable {
		l.logger.Warn("Bandwidth limits disabled")
		return l, nil
	}

	if config.EgressBitsPerSec == 0 {
		return nil, errors.New("invalid config: egress_bits_per_sec must be non-zero")
	}
	if config.IngressBitsPerSec == 0 {
		return nil, errors.New("invalid config: ingress_bits_per_sec must be non-zero")
	}

	l.logger.Infof("Setting egress bandwidth to %s/sec", memsize.BitFormat(config.EgressBitsPerSec))
	l.logger.Infof("Setting ingress bandwidth to %s/sec", memsize.BitFormat(config.IngressBitsPerSec))

	etps := config.EgressBitsPerSec / config.TokenSize
	itps := config.IngressBitsPerSec / config.TokenSize

	l.egress = rate.NewLimiter(rate.Limit(etps), int(etps))
	l.ingress = rate.NewLimiter(rate.Limit(itps), int(itps))

	return l, nil
}

func (l *Limiter) reserve(rl *rate.Limiter, nbytes int64) error {
	if !l.config.Enable {
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

// Adjust divides the originally configured egress and ingress bps by denominator.
// Note, because the original configuration is always used, multiple Adjust calls
// have no affect on each other.
func (l *Limiter) Adjust(denominator int) error {
	if denominator <= 0 {
		return errors.New("denominator must be greater than 0")
	}

	ebps := max(l.config.EgressBitsPerSec/l.config.TokenSize/uint64(denominator), 1)
	ibps := max(l.config.IngressBitsPerSec/l.config.TokenSize/uint64(denominator), 1)

	l.egress.SetLimit(rate.Limit(ebps))
	l.ingress.SetLimit(rate.Limit(ibps))

	return nil
}

// EgressLimit returns the current egress limit.
func (l *Limiter) EgressLimit() int64 {
	return int64(l.egress.Limit())
}

// IngressLimit returns the current ingress limit.
func (l *Limiter) IngressLimit() int64 {
	return int64(l.ingress.Limit())
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
