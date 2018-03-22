package scheduler

import (
	"testing"

	"code.uber.internal/infra/kraken/utils/memsize"
)

func TestGetBucket(t *testing.T) {
	tests := []struct {
		desc     string
		size     uint64
		expected uint64
	}{
		{"below min", memsize.KB, memsize.MB},
		{"above max", 30 * memsize.GB, 20 * memsize.GB},
		{"between buckets", 600 * memsize.MB, 500 * memsize.MB},
		{"exact bucket", memsize.GB, memsize.GB},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			result := getBucket(test.size)
			if test.expected != result {
				t.Fatalf("Expected %s, got %s", memsize.Format(test.expected), memsize.Format(result))
			}
		})
	}
}
