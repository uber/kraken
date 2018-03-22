package scheduler

import (
	"code.uber.internal/infra/kraken/utils/memsize"
)

var _buckets = [...]uint64{
	memsize.MB,
	10 * memsize.MB,
	50 * memsize.MB,
	100 * memsize.MB,
	250 * memsize.MB,
	500 * memsize.MB,
	750 * memsize.MB,
	memsize.GB,
	memsize.GB + 250*memsize.MB,
	memsize.GB + 500*memsize.MB,
	memsize.GB + 750*memsize.MB,
	2 * memsize.GB,
	3 * memsize.GB,
	4 * memsize.GB,
	6 * memsize.GB,
	8 * memsize.GB,
	12 * memsize.GB,
	16 * memsize.GB,
	20 * memsize.GB,
}

func getBucket(size uint64) uint64 {
	b := _buckets[0]
	for i := 1; i < len(_buckets); i++ {
		if size < _buckets[i] {
			break
		}
		b = _buckets[i]
	}
	return b
}
