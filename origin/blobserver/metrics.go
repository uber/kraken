package blobserver

import (
	"time"

	"github.com/uber-go/tally"
)

type metrics struct {
	replicateBlobLatency tally.Histogram
	replicateBlobErrors  tally.Counter
}

func newMetrics(s tally.Scope) *metrics {
	return &metrics{
		replicateBlobLatency: s.Histogram(
			"replicate_blob",
			tally.MustMakeExponentialDurationBuckets(10*time.Millisecond, 2, 18)), // 10ms-21.8m
		replicateBlobErrors: s.Counter("replicate_blob_errors"),
	}
}
