package testutil

import (
	"fmt"
	"net"
	"net/http"
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
