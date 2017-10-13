package netutil

import "time"

import "strings"
import "fmt"

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

// SplitHostPort parses address hostname:port or hostname to host and port
func SplitHostPort(addr string) (host string, port string, err error) {
	if addr == ":" {
		return "", "", fmt.Errorf(": is not a valid address")
	}

	strs := strings.Split(addr, ":")
	if len(strs) == 1 {
		return strs[0], "", nil
	}
	if len(strs) == 2 && strs[1] != "" {
		return strs[0], strs[1], nil
	}
	return "", "", fmt.Errorf("%s is not a valid address", addr)
}
