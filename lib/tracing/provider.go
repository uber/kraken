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
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc" // Changed from otlptracehttp
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// InitProvider initializes the OpenTelemetry trace provider with OTLP exporter.
// Returns a shutdown function that should be called on application exit.
// The exporter sends traces to Jaeger via OTLP HTTP (Jaeger supports OTLP natively since v1.35).
func InitProvider(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	cfg = cfg.applyDefaults()

	if !cfg.Enabled {
		// Return no-op shutdown function if tracing is disabled
		return func(ctx context.Context) error { return nil }, nil
	}

	if cfg.ServiceName == "" {
		return nil, fmt.Errorf("tracing enabled but service_name not configured")
	}

	// Create OTLP HTTP exporter (Jaeger supports OTLP on port 4318)
	endpoint := fmt.Sprintf("%s:%d", cfg.AgentHost, cfg.AgentPort)
	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(), // Use HTTP instead of HTTPS
	)

	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %s", err)
	}

	// Create resource that identifies this service
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %s", err)
	}

	// Create trace provider with batching and sampling
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(res),
		trace.WithSampler(trace.ParentBased(
			trace.TraceIDRatioBased(cfg.SamplingRate),
		)),
	)

	// Set as global trace provider
	otel.SetTracerProvider(tp)

	// Set W3C Trace Context propagator for cross-service context propagation
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}
