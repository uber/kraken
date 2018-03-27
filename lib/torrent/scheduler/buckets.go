package scheduler

import (
	"code.uber.internal/infra/kraken/utils/memsize"
)

var _buckets = [...]uint64{
	10 * memsize.MB,
	100 * memsize.MB,
	memsize.GB,
	10 * memsize.GB,
}

func diff(x, y uint64) uint64 {
	if x < y {
		return y - x
	}
	return x - y
}

// getBucket rounds size to the nearest bucket.
func getBucket(size uint64) uint64 {
	var a, b uint64
	for i := 1; i < len(_buckets); i++ {
		a, b = _buckets[i-1], _buckets[i]
		if diff(size, a) < diff(size, b) {
			return a
		}
	}
	return b
}
