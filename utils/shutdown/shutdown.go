// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package shutdown

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/uber/kraken/utils/log"
)

// Handler manages graceful shutdown of the application.
type Handler struct {
	ctx        context.Context
	cancel     context.CancelFunc
	cleanupFns []func() error
	mu         sync.Mutex
	once       sync.Once
}

// New creates a new shutdown handler with a cancellable context.
// It also sets up signal handlers for SIGINT and SIGTERM.
func New(ctx context.Context) *Handler {
	ctx, cancel := context.WithCancel(ctx)
	h := &Handler{
		ctx:        ctx,
		cancel:     cancel,
		cleanupFns: make([]func() error, 0),
	}

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-sigChan:
			log.Infof("Received signal %v, initiating graceful shutdown", sig)
			h.Shutdown()
		case <-ctx.Done():
			return
		}
	}()

	return h
}

// Context returns the handler's context.
func (h *Handler) Context() context.Context {
	return h.ctx
}

// AddCleanup adds a cleanup function to be called during shutdown.
// Cleanup functions are called in reverse order (LIFO).
func (h *Handler) AddCleanup(fn func() error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cleanupFns = append(h.cleanupFns, fn)
}

// Shutdown initiates a graceful shutdown by cancelling the context
// and calling all registered cleanup functions.
func (h *Handler) Shutdown() {
	h.once.Do(func() {
		log.Info("Initiating shutdown...")
		h.cancel()

		h.mu.Lock()
		defer h.mu.Unlock()

		// Call cleanup functions in reverse order (LIFO)
		for i := len(h.cleanupFns) - 1; i >= 0; i-- {
			if err := h.cleanupFns[i](); err != nil {
				log.Errorf("Error during cleanup: %s", err)
			}
		}
		log.Info("Shutdown complete")
	})
}

// Exit logs the error and initiates graceful shutdown, then exits with the given code.
func (h *Handler) Exit(err error, code int) {
	if err != nil {
		log.Errorf("Fatal error: %s", err)
	}
	h.Shutdown()
	os.Exit(code)
}

// Exitf logs a formatted error message and initiates graceful shutdown, then exits with the given code.
func (h *Handler) Exitf(code int, format string, args ...interface{}) {
	log.Errorf(format, args...)
	h.Shutdown()
	os.Exit(code)
}
