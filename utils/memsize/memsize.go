package memsize

import "fmt"

// Defines the number of bytes in each unit.
const (
	B uint64 = 1 << (10 * iota)
	KB
	MB
	GB
	TB
)

// Format returns a human readable representation for the given number of bytes.
func Format(b uint64) string {
	units := []struct {
		d uint64
		s string
	}{
		{TB, "TB"},
		{GB, "GB"},
		{MB, "MB"},
		{KB, "KB"},
		{B, "B"},
	}
	for _, u := range units {
		if b >= u.d {
			f := float64(b) / float64(u.d)
			return fmt.Sprintf("%.2f%s", f, u.s)
		}
	}
	return "0B"
}
