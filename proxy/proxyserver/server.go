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
package proxyserver

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/lib/tracing"
	"github.com/uber/kraken/utils/listener"
	"github.com/uber/kraken/utils/log"

	"github.com/go-chi/chi"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/lib/middleware"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/handler"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Server defines the proxy HTTP server.
type Server struct {
	stats           tally.Scope
	preheatHandler  *PreheatHandler
	prefetchHandler *PrefetchHandler
	config          Config
}

// New creates a new Server.
func New(
	stats tally.Scope,
	config Config,
	client blobclient.ClusterClient,
	tagClient tagclient.Client,
	synchronous bool,
) *Server {
	return &Server{
		stats,
		NewPreheatHandler(client, synchronous),
		NewPrefetchHandler(client, tagClient, &DefaultTagParser{}, stats, int64(config.PrefetchMinBlobSize), int64(config.PrefetchMaxBlobSize), synchronous),
		config,
	}
}

// traceHeadersMiddleware logs incoming trace headers BEFORE otelhttp processes them.
func traceHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.With(
			"path", r.URL.Path,
			"traceparent", r.Header.Get("traceparent"),
			"tracestate", r.Header.Get("tracestate"),
			"jaeger-debug-id", r.Header.Get("jaeger-debug-id"),
			"uber-trace-id", r.Header.Get("uber-trace-id"),
		).Info("[TRACE DEBUG] Incoming request headers")
		next.ServeHTTP(w, r)
	})
}

// traceSpanMiddleware logs span context AFTER otelhttp creates the span.
func traceSpanMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check span from otelhttp
		spanCtx := trace.SpanContextFromContext(r.Context())
		log.With(
			"path", r.URL.Path,
			"trace_id", spanCtx.TraceID().String(),
			"span_id", spanCtx.SpanID().String(),
			"is_sampled", spanCtx.IsSampled(),
			"is_valid", spanCtx.IsValid(),
			"is_remote", spanCtx.IsRemote(),
		).Info("[TRACE DEBUG] Span context from otelhttp")

		// Manually test creating a span with the global tracer
		tp := otel.GetTracerProvider()
		tracer := tp.Tracer("kraken-proxy-debug")
		ctx, testSpan := tracer.Start(r.Context(), "debug-test-span")
		testSpanCtx := testSpan.SpanContext()
		log.With(
			"tracer_provider_type", fmt.Sprintf("%T", tp),
			"tracer_type", fmt.Sprintf("%T", tracer),
			"test_trace_id", testSpanCtx.TraceID().String(),
			"test_span_id", testSpanCtx.SpanID().String(),
			"test_is_sampled", testSpanCtx.IsSampled(),
			"test_is_valid", testSpanCtx.IsValid(),
			"test_is_recording", testSpan.IsRecording(),
		).Info("[TRACE DEBUG] Manual span creation test")
		testSpan.End()

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	// Debug: log incoming trace headers (REMOVE AFTER DEBUGGING)
	r.Use(traceHeadersMiddleware)

	// Tracing middleware creates spans
	r.Use(tracing.HTTPMiddleware("kraken-proxy"))

	// Debug: log span context after otelhttp (REMOVE AFTER DEBUGGING)
	r.Use(traceSpanMiddleware)

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", handler.Wrap(s.healthHandler))

	r.Post("/registry/notifications", handler.Wrap(s.preheatHandler.Handle))
	r.Post("/proxy/v1/registry/prefetch", s.prefetchHandler.HandleV1)
	r.Post("/proxy/v2/registry/prefetch", s.prefetchHandler.HandleV2)

	// Serves /debug/pprof endpoints.
	r.Mount("/", http.DefaultServeMux)

	return r
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) error {
	if _, err := fmt.Fprintln(w, "OK"); err != nil {
		return fmt.Errorf("write response: %s", err)
	}
	return nil
}

func (s *Server) ListenAndServe() error {
	log.Infof("Starting proxy server on %s", s.config.Listener)
	return listener.Serve(s.config.Listener, s.Handler())
}
