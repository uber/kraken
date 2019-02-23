// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package testutil

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
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
		panic(err)
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

// TempFile creates a temporary file. Returns its name and cleanup function.
func TempFile(data []byte) (string, func()) {
	var cleanup Cleanup
	defer cleanup.Recover()

	f, err := ioutil.TempFile(".", "")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.Remove(f.Name()) })
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		panic(err)
	}

	return f.Name(), cleanup.Run
}
