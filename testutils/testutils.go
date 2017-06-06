package testutils

import (
	"io/ioutil"
	"net/http"
	"testing"
)

// RequireStatus fails if the response is not of the given status. Logs the body
// of the response on failure for debugging purposes.
func RequireStatus(t *testing.T, r *http.Response, status int) {
	if r.StatusCode != status {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatalf(
				"Expected status %d, got %d. Could not read body: %v",
				status, r.StatusCode, err)
		}
		t.Fatalf(
			"Expected status %d, got %d. Body: %s",
			status, r.StatusCode, string(b))
	}
}
