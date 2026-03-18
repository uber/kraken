package blobserver

import (
	"github.com/uber-go/tally"
)

type metrics struct {
	replicateBlobTimer  tally.Timer
	replicateBlobErrors tally.Counter
}

func newMetrics(s tally.Scope) *metrics {
	return &metrics{
		replicateBlobTimer:  s.Timer("replicate_blob"),
		replicateBlobErrors: s.Counter("replicate_blob_errors"),
	}
}
