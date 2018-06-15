package scheduler

import (
	"testing"
	"time"

	"code.uber.internal/infra/kraken/utils/memsize"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestGetBucket(t *testing.T) {
	tests := []struct {
		desc     string
		size     uint64
		expected string
	}{
		{"below min", memsize.MB, "xsmall"},
		{"above max", 30 * memsize.GB, "xxlarge"},
		{"between buckets", 3 * memsize.GB, "large"},
		{"exact bucket", 1 * memsize.GB, "medium"},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require.Equal(t, test.expected, getBucket(test.size).name)
		})
	}
}

func TestBucketAddRange(t *testing.T) {
	tests := []struct {
		desc               string
		start, stop, width time.Duration
		expected           []time.Duration
	}{
		{"normal", 1, 4, 1, []time.Duration{1, 2, 3}},
		{"single", 1, 4, 5, []time.Duration{1}},
		{"none", 4, 1, 1, nil},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			b := newBucket("test", 0)
			b.addRange(test.start, test.stop, test.width)
			require.Equal(t, tally.DurationBuckets(test.expected), b.durations)
		})
	}
}
