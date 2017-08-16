package testutil

import (
	"fmt"
	"time"
)

// PollUntilTrue calls f until f returns true. Returns error if true is not received
// within timeout.
func PollUntilTrue(timeout time.Duration, f func() bool) error {
	timer := time.NewTimer(timeout)
	for {
		result := make(chan bool, 1)
		go func() {
			result <- f()
		}()
		select {
		case ok := <-result:
			if ok {
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		case <-timer.C:
			return fmt.Errorf("timed out after %.2f seconds", timeout.Seconds())
		}
	}
}
