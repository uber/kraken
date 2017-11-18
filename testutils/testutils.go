package testutils

import (
	"io/ioutil"
	"net"
	"net/http"
	"testing"
)

// Cleanup contains a list of function that are called to cleanup a fixture
type Cleanup struct {
	funcs []func()
}

// Add adds function to funcs list
func (c *Cleanup) Add(f ...func()) {
	c.funcs = append(c.funcs, f...)
}

// AppendFront append funcs from another cleanup in front of the funcs list
func (c *Cleanup) AppendFront(c1 *Cleanup) {
	c.funcs = append(c1.funcs, c.funcs...)
}

// Recover runs cleanup functions after test exit with exception
func (c *Cleanup) Recover() {
	if err := recover(); err != nil {
		c.run()
	}
}

// Run runs cleanup functions when a test finishes running
func (c *Cleanup) Run() {
	c.run()
}

func (c *Cleanup) run() {
	for _, f := range c.funcs {
		f()
	}
}

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

// StartServer starts an HTTP server with h. Returns address the server is
// listening on, and a closure for stopping the server.
func StartServer(h http.Handler) (addr string, stop func()) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	s := &http.Server{Handler: h}
	go s.Serve(l)
	return l.Addr().String(), func() { s.Close() }
}
