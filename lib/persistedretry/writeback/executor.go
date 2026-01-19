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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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
	tracer   trace.Tracer
}

// NewExecutor creates a new Executor.
func NewExecutor(
	stats tally.Scope,
	fs FileStore,
	backends *backend.Manager,
	tracer trace.Tracer,
) *Executor {
	stats = stats.Tagged(map[string]string{
		"module": "writebackexecutor",
	})

	return &Executor{stats, fs, backends, tracer}
}

// Name returns the executor name.
func (e *Executor) Name() string {
	return "writeback"
}

// buildSpanOptions creates span options for the writeback execution span.
// If the task has trace context from the original request, it adds a span link
// to enable navigation between the async writeback and the original request in Jaeger.
// It also records whether the original request was sampled for debugging.
func buildSpanOptions(t *Task) []trace.SpanStartOption {
	opts := []trace.SpanStartOption{
		trace.WithAttributes(
			attribute.String("namespace", t.Namespace),
			attribute.String("blob.name", t.Name),
			attribute.Int("task.failures", t.Failures),
			attribute.Int64("task.age_ms", time.Since(t.CreatedAt).Milliseconds()),
		),
	}

	if t.HasTraceContext() {
		if originSpanCtx := t.SpanContext(); originSpanCtx.IsValid() {
			opts = append(opts,
				trace.WithLinks(trace.Link{
					SpanContext: originSpanCtx,
					Attributes: []attribute.KeyValue{
						attribute.String("link.type", "origin_request"),
					},
				}),
				trace.WithAttributes(
					attribute.String("origin.trace_id", t.TraceID),
					attribute.Bool("origin.sampled", originSpanCtx.IsSampled()),
				),
			)
		}
	}

	return opts
}

// Exec uploads the cache file corresponding to r's digest to the remote backend
// that matches r's namespace.
func (e *Executor) Exec(r persistedretry.Task) error {
	t, ok := r.(*Task)
	if !ok {
		return fmt.Errorf("expected *Task, got %T", r)
	}

	ctx, span := e.tracer.Start(context.Background(), "writeback.exec", buildSpanOptions(t)...)
	defer span.End()

	if err := e.upload(ctx, t); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	err := e.fs.DeleteCacheFileMetadata(t.Name, &metadata.Persist{})
	if err != nil && !os.IsNotExist(err) {
		span.RecordError(err)
		span.SetStatus(codes.Error, "delete persist metadata failed")
		return fmt.Errorf("delete persist metadata: %s", err)
	}

	span.SetStatus(codes.Ok, "writeback completed")
	return nil
}

func (e *Executor) upload(ctx context.Context, t *Task) error {
	ctx, span := e.tracer.Start(ctx, "writeback.upload",
		trace.WithAttributes(
			attribute.String("blob.name", t.Name),
		),
	)
	defer span.End()

	start := time.Now()

	log.WithTraceContext(ctx).With("namespace", t.Namespace, "name", t.Name).Info("Uploading cache file to the remote backend")
	client, err := e.backends.GetClient(t.Namespace)
	if err != nil {
		if err == backend.ErrNamespaceNotFound {
			log.WithTraceContext(ctx).With(
				"namespace", t.Namespace,
				"name", t.Name).Info("Dropping writeback for unconfigured namespace")
			span.SetAttributes(attribute.Bool("upload.dropped", true))
			span.SetAttributes(attribute.String("upload.drop_reason", "namespace_not_found"))
			return nil
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "get client failed")
		return fmt.Errorf("get client: %s", err)
	}

	// Check if already uploaded (stat check)
	if _, err := client.Stat(t.Namespace, t.Name); err == nil {
		// File already uploaded, no-op.
		log.WithTraceContext(ctx).With("namespace", t.Namespace, "name", t.Name).Debug("Blob already exists in backend, skipping upload")
		span.SetAttributes(attribute.Bool("upload.skipped", true))
		span.SetAttributes(attribute.String("upload.skip_reason", "already_exists"))
		return nil
	}

	f, err := e.fs.GetCacheFileReader(t.Name)
	if err != nil {
		if os.IsNotExist(err) {
			// Nothing we can do about this but make noise and drop the task.
			e.stats.Counter("missing_files").Inc(1)
			log.WithTraceContext(ctx).With("name", t.Name).Error("Invariant violation: writeback cache file missing")
			span.SetAttributes(attribute.Bool("upload.dropped", true))
			span.SetAttributes(attribute.String("upload.drop_reason", "file_missing"))
			span.RecordError(err)
			return nil
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "get file failed")
		return fmt.Errorf("get file: %s", err)
	}
	defer closers.Close(f)

	if err := client.Upload(t.Namespace, t.Name, f); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "upload failed")
		return fmt.Errorf("upload: %s", err)
	}

	uploadDuration := time.Since(start)
	log.WithTraceContext(ctx).With("namespace", t.Namespace, "name", t.Name, "duration_ms", uploadDuration.Milliseconds()).Info("Uploaded cache file to remote backend")

	span.SetAttributes(
		attribute.Int64("upload.duration_ms", uploadDuration.Milliseconds()),
		attribute.Int64("task.lifetime_ms", time.Since(t.CreatedAt).Milliseconds()),
		attribute.Bool("upload.success", true),
	)
	span.SetStatus(codes.Ok, "upload completed")

	// We don't want to time noops nor errors.
	e.stats.Timer("upload").Record(uploadDuration)
	e.stats.Timer("lifetime").Record(time.Since(t.CreatedAt))

	return nil
}
