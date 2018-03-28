package scheduler

import (
	"testing"

	"code.uber.internal/infra/kraken/utils/memsize"
)

func TestGetSizeBucket(t *testing.T) {
	tests := []struct {
		desc     string
		size     uint64
		expected uint64
	}{
		{"below min", memsize.KB, 10 * memsize.MB},
		{"above max", 30 * memsize.GB, 10 * memsize.GB},
		{"between buckets", 500 * memsize.MB, memsize.GB},
		{"exact bucket", memsize.GB, memsize.GB},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			result := getSizeBucket(test.size)
			if test.expected != result {
				t.Fatalf("Expected %s, got %s", memsize.Format(test.expected), memsize.Format(result))
			}
		})
	}
}
