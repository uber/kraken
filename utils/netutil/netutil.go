package netutil

import "time"

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// WithRetry executes f maxRetries times until it returns non-nil error, sleeping
// for the given delay between retries with exponential backoff until maxDelay is
// reached.
func WithRetry(maxRetries uint, delay time.Duration, maxDelay time.Duration, f func() error) error {
	var retries uint
	for {
		err := f()
		if err == nil {
			return nil
		}
		if retries > maxRetries {
			return err
		}
		time.Sleep(min(delay*(1<<retries), maxDelay))
		retries++
	}
}
