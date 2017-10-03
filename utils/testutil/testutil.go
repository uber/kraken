package testutil

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
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

// ExpectBody fails if the response does not have the expected body.
func ExpectBody(t *testing.T, resp *http.Response, expected []byte) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Could not read body: %s", err)
	}
	if string(expected) != string(body) {
		t.Fatalf("Expected body %q, got %q", string(expected), string(body))
	}
}
