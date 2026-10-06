package memory

import (
	"errors"
	"math"
	"runtime/debug"
)

// Config configures [Store].
type Config struct {
	// GOMEMLIMITBytes and GOGC tune the Go GC. In short, it is advised to set GOMEMLIMIT to 90-95% of the container's reserved memory
	// and to turn GOGC off, assuming the container has reserved memory. More info on tuning the Go GC: https://go.dev/doc/gc-guide.
	//
	// If set, they overwrite any previous configurations for GOMEMLIMIT and GOGC.
	// GOMEMLIMIT *must* be configured either through [Config] or another way (e.g. an env var).
	// GOGC defaults to the Go default (100), if not set and is turned off if a negative value is provided.
	GOMEMLIMITBytes int64 `yaml:"gomemlimit_bytes"`
	// Check the comment at [Config.GOMEMLIMITBytes].
	GOGC int `yaml:"gogc"`
	// CapacityBytes sets a *hard* limit on the memory [Store] can keep reachable (i.e. unreclaimable by the GC) to store blobs.
	// Any other memory that [Store] uses (e.g. storing metadata) is NOT capped by CapacityBytes.
	CapacityBytes uint64 `yaml:"capacity_bytes"`
}

func (c *Config) applyDefaults() error {
	if c.CapacityBytes <= 0 {
		return errors.New("capacity_bytes must be explicitly set")
	}
	return nil
}

func (c *Config) configureGC() error {
	if c.GOMEMLIMITBytes != 0 {
		debug.SetMemoryLimit(c.GOMEMLIMITBytes)
	} else {
		currentLimit := debug.SetMemoryLimit(-1)
		notSet := currentLimit == math.MaxInt64
		if notSet {
			return errors.New("GOMEMLIMIT must be configured either through memory.Config or an env var")
		}
	}

	if c.GOGC != 0 {
		debug.SetGCPercent(c.GOGC)
	}
	return nil
}
