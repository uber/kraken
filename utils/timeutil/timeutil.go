package timeutil

import "time"

// MostRecent returns the most recent Time of ts.
func MostRecent(ts ...time.Time) time.Time {
	if len(ts) == 0 {
		return time.Time{}
	}
	max := ts[0]
	for i := 1; i < len(ts); i++ {
		if max.Before(ts[i]) {
			max = ts[i]
		}
	}
	return max
}

// MaxDuration returns the largest duration between a and b.
func MaxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
