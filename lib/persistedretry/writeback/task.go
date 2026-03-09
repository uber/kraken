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
package writeback

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/uber/kraken/core"
)

// Task contains information to write back a blob to remote storage.
type Task struct {
	Namespace   string        `db:"namespace"`
	Name        string        `db:"name"`
	CreatedAt   time.Time     `db:"created_at"`
	LastAttempt time.Time     `db:"last_attempt"`
	Failures    int           `db:"failures"`
	Delay       time.Duration `db:"delay"`

	// Trace context for linking async execution back to original request.
	TraceID    string `db:"trace_id"`
	SpanID     string `db:"span_id"`
	TraceFlags string `db:"trace_flags"` // Hex string of trace flags (e.g., "01" if sampled)

	// Deprecated. Use name instead.
	Digest core.Digest `db:"digest"`
}

// NewTask creates a new Task.
// Deprecated: Use NewTaskWithContext to preserve trace context.
func NewTask(namespace, name string, delay time.Duration) *Task {
	return &Task{
		Namespace: namespace,
		Name:      name,
		CreatedAt: time.Now(),
		Delay:     delay,
	}
}

// NewTaskWithContext creates a new Task and captures the trace context from ctx.
// This allows the async writeback execution to be linked to the original request trace.
// It also captures TraceFlags to preserve the sampling decision.
func NewTaskWithContext(ctx context.Context, namespace, name string, delay time.Duration) *Task {
	t := &Task{
		Namespace: namespace,
		Name:      name,
		CreatedAt: time.Now(),
		Delay:     delay,
	}
	if spanCtx := trace.SpanContextFromContext(ctx); spanCtx.IsValid() {
		t.TraceID = spanCtx.TraceID().String()
		t.SpanID = spanCtx.SpanID().String()
		t.TraceFlags = spanCtx.TraceFlags().String()
	}

	return t
}

// HasTraceContext returns true if the task has captured trace context.
func (t *Task) HasTraceContext() bool {
	return t.TraceID != "" && t.SpanID != ""
}

// SpanContext reconstructs a trace.SpanContext from the stored trace IDs.
// Returns an invalid SpanContext if the task has no trace context or if parsing fails.
func (t *Task) SpanContext() trace.SpanContext {
	if !t.HasTraceContext() {
		return trace.SpanContext{}
	}

	traceID, err := trace.TraceIDFromHex(t.TraceID)
	if err != nil {
		return trace.SpanContext{}
	}

	spanID, err := trace.SpanIDFromHex(t.SpanID)
	if err != nil {
		return trace.SpanContext{}
	}

	// Parse TraceFlags to preserve sampling decision
	var traceFlags trace.TraceFlags
	if t.TraceFlags != "" {
		traceFlags = trace.TraceFlags(parseHexByte(t.TraceFlags))
	}

	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: traceFlags,
		Remote:     true,
	})
}

// parseHexByte parses a hex string (e.g., "01") to a byte.
func parseHexByte(s string) byte {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 1 {
		return 0
	}
	return b[0]
}

func (t *Task) String() string {
	return fmt.Sprintf("writeback.Task(namespace=%s, name=%s)", t.Namespace, t.Name)
}

// GetLastAttempt returns when t was last attempted.
func (t *Task) GetLastAttempt() time.Time {
	return t.LastAttempt
}

// GetFailures returns the number of times t has failed.
func (t *Task) GetFailures() int {
	return t.Failures
}

// Ready returns whether t is ready to run.
func (t *Task) Ready() bool {
	return time.Since(t.CreatedAt) >= t.Delay
}

// Tags is unused.
func (t *Task) Tags() map[string]string {
	return nil
}
