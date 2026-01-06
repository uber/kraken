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
package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/uber/kraken"

// StartSpan creates a new span as a child of any existing span in the context.
// Returns the new context (with span) and a function to end the span.
//
// Usage:
//
//	ctx, endSpan := tracing.StartSpan(ctx, "operation-name")
//	defer endSpan()
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, func()) {
	ctx, span := otel.Tracer(tracerName).Start(ctx, name, opts...)
	return ctx, func() { span.End() }
}

// StartSpanWithAttributes creates a span with initial attributes.
func StartSpanWithAttributes(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func()) {
	ctx, span := otel.Tracer(tracerName).Start(ctx, name, trace.WithAttributes(attrs...))
	return ctx, func() { span.End() }
}

// SpanFromContext returns the current span from context, or a no-op span if none exists.
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// SetSpanAttributes adds attributes to the current span in context.
func SetSpanAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attrs...)
}

// RecordSpanError records an error on the current span and sets status to Error.
func RecordSpanError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// SetSpanOK marks the current span as successful.
func SetSpanOK(ctx context.Context) {
	span := trace.SpanFromContext(ctx)
	span.SetStatus(codes.Ok, "")
}

// Common attribute keys for Kraken spans
var (
	AttrNamespace = attribute.Key("kraken.namespace")
	AttrDigest    = attribute.Key("kraken.digest")
	AttrRepo      = attribute.Key("kraken.repo")
	AttrTag       = attribute.Key("kraken.tag")
	AttrBlobSize  = attribute.Key("kraken.blob_size")
	AttrPeerID    = attribute.Key("kraken.peer_id")
)

