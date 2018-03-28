package scheduler

import (
	"time"

	"code.uber.internal/infra/kraken/utils/memsize"
	"github.com/uber-go/tally"
)

var _sizeBuckets = [...]uint64{
	10 * memsize.MB,
	100 * memsize.MB,
	memsize.GB,
	10 * memsize.GB,
}

// getSizeBucket rounds size up to the nearest bucket.
func getSizeBucket(size uint64) (b uint64) {
	for _, b = range _sizeBuckets {
		if b >= size {
			break
		}
	}
	return b
}

var _durationBuckets = tally.DurationBuckets{
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	750 * time.Millisecond,
	time.Second,
	2 * time.Second,
	3 * time.Second,
	4 * time.Second,
	5 * time.Second,
	6 * time.Second,
	7 * time.Second,
	8 * time.Second,
	9 * time.Second,
	10 * time.Second,
	12 * time.Second,
	14 * time.Second,
	16 * time.Second,
	18 * time.Second,
	20 * time.Second,
	25 * time.Second,
	30 * time.Second,
	35 * time.Second,
	40 * time.Second,
	45 * time.Second,
	50 * time.Second,
	55 * time.Second,
	time.Minute,
	2 * time.Minute,
	3 * time.Minute,
	4 * time.Minute,
	5 * time.Minute,
	10 * time.Minute,
	15 * time.Minute,
	20 * time.Minute,
	25 * time.Minute,
	30 * time.Minute,
}

func recordDownloadTime(stats tally.Scope, size int64, t time.Duration) {
	stats.Tagged(map[string]string{
		"size":    memsize.Format(getSizeBucket(uint64(size))),
		"version": "2", // Until old stats expire.
	}).Histogram("download_time", _durationBuckets).RecordDuration(t)
}
