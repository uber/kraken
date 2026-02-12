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
	"fmt"
	"os"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/persistedretry"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/log"

	"go.opentelemetry.io/otel/trace"
)

// FileStore defines store operations required for write-back.
type FileStore interface {
	DeleteCacheFileMetadata(name string, md metadata.Metadata) error
	GetCacheFileReader(name string) (store.FileReader, error)
}

// Executor executes write back tasks.
type Executor struct {
	stats    tally.Scope
	fs       FileStore
	backends *backend.Manager
}

// NewExecutor creates a new Executor.
func NewExecutor(
	stats tally.Scope,
	fs FileStore,
	backends *backend.Manager,
) *Executor {
	stats = stats.Tagged(map[string]string{
		"module": "writebackexecutor",
	})

	return &Executor{stats, fs, backends}
}

// Name returns the executor name.
func (e *Executor) Name() string {
	return "writeback"
}

// Exec uploads the cache file corresponding to r's digest to the remote backend
// that matches r's namespace.
func (e *Executor) Exec(r persistedretry.Task) error {
	t, ok := r.(*Task)
	if !ok {
		return fmt.Errorf("expected *Task, got %T", r)
	}

	// Extract context from task for trace propagation
	// Tasks from public endpoints will have trace context, internal tasks may not
	ctx := e.getContextFromTask(t)

	log.WithTraceContext(ctx).With(
		"namespace", t.Namespace,
		"name", t.Name,
		"has_trace_context", t.HasTraceContext(),
	).Debug("Executing writeback task")

	if err := e.upload(ctx, t); err != nil {
		log.WithTraceContext(ctx).With(
			"namespace", t.Namespace,
			"name", t.Name,
			"error", err,
		).Error("Failed to upload during writeback")
		return err
	}

	err := e.fs.DeleteCacheFileMetadata(t.Name, &metadata.Persist{})
	if err != nil && !os.IsNotExist(err) {
		log.WithTraceContext(ctx).With(
			"namespace", t.Namespace,
			"name", t.Name,
			"error", err,
		).Error("Failed to delete persist metadata")
		return fmt.Errorf("delete persist metadata: %s", err)
	}

	log.WithTraceContext(ctx).With(
		"namespace", t.Namespace,
		"name", t.Name,
	).Debug("Successfully completed writeback task")

	return nil
}

// getContextFromTask extracts trace context from a task if available.
// Returns context.Background() if no trace context is present.
func (e *Executor) getContextFromTask(t *Task) context.Context {
	if !t.HasTraceContext() {
		return context.Background()
	}

	spanCtx := t.SpanContext()
	if !spanCtx.IsValid() {
		return context.Background()
	}

	// Create a context with the span context for logging correlation
	return trace.ContextWithSpanContext(context.Background(), spanCtx)
}

func (e *Executor) upload(ctx context.Context, t *Task) error {
	start := time.Now()

	log.WithTraceContext(ctx).With(
		"namespace", t.Namespace,
		"name", t.Name,
	).Info("Uploading cache file to the remote backend")

	client, err := e.backends.GetClient(t.Namespace)
	if err != nil {
		if err == backend.ErrNamespaceNotFound {
			log.WithTraceContext(ctx).With(
				"namespace", t.Namespace,
				"name", t.Name,
			).Info("Dropping writeback for unconfigured namespace")
			return nil
		}
		log.WithTraceContext(ctx).With(
			"namespace", t.Namespace,
			"name", t.Name,
			"error", err,
		).Error("Failed to get backend client")
		return fmt.Errorf("get client: %s", err)
	}

	if _, err := client.Stat(t.Namespace, t.Name); err == nil {
		// File already uploaded, no-op.
		log.WithTraceContext(ctx).With(
			"namespace", t.Namespace,
			"name", t.Name,
		).Debug("File already exists in backend, skipping upload")
		return nil
	}

	f, err := e.fs.GetCacheFileReader(t.Name)
	if err != nil {
		if os.IsNotExist(err) {
			// Nothing we can do about this but make noise and drop the task.
			e.stats.Counter("missing_files").Inc(1)
			log.WithTraceContext(ctx).With(
				"namespace", t.Namespace,
				"name", t.Name,
			).Error("Invariant violation: writeback cache file missing")
			return nil
		}
		log.WithTraceContext(ctx).With(
			"namespace", t.Namespace,
			"name", t.Name,
			"error", err,
		).Error("Failed to get cache file reader")
		return fmt.Errorf("get file: %s", err)
	}
	defer closers.Close(f)

	log.WithTraceContext(ctx).With(
		"namespace", t.Namespace,
		"name", t.Name,
	).Debug("Starting backend upload")

	if err := client.Upload(t.Namespace, t.Name, f); err != nil {
		log.WithTraceContext(ctx).With(
			"namespace", t.Namespace,
			"name", t.Name,
			"error", err,
		).Error("Backend upload failed")
		return fmt.Errorf("upload: %s", err)
	}

	log.WithTraceContext(ctx).With(
		"namespace", t.Namespace,
		"name", t.Name,
	).Info("Uploaded cache file to remote backend")

	// We don't want to time noops nor errors.
	e.stats.Timer("upload").Record(time.Since(start))
	e.stats.Timer("lifetime").Record(time.Since(t.CreatedAt))

	return nil
}
