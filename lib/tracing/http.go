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
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"

	"github.com/uber/kraken/utils/log"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
)

var debugTracerOnce sync.Once

// debugGlobalTracer logs information about the global tracer provider.
// Called once when HTTPMiddleware is first used.
func debugGlobalTracer(serviceName string) {
	debugTracerOnce.Do(func() {
		tp := otel.GetTracerProvider()
		tpType := fmt.Sprintf("%T", tp)

		// Get a tracer to check if it's functional
		tracer := tp.Tracer(serviceName)
		tracerType := fmt.Sprintf("%T", tracer)

		log.With(
			"tracer_provider_type", tpType,
			"tracer_type", tracerType,
			"service_name", serviceName,
		).Info("[TRACE DEBUG] Global TracerProvider info")

		// Check if it's a noop tracer (means jaegerfx didn't set it up)
		if tpType == "*trace.noopTracerProvider" || tpType == "trace.noopTracerProvider" {
			log.Warn("[TRACE DEBUG] WARNING: Using no-op tracer! jaegerfx may not have initialized the global tracer.")
		}
	})
}

// HTTPMiddleware returns a middleware that traces incoming HTTP requests.
// The serviceName is used to identify spans from this service in Jaeger.
func HTTPMiddleware(serviceName string) func(http.Handler) http.Handler {
	// Debug: log tracer provider info once
	debugGlobalTracer(serviceName)

	return func(next http.Handler) http.Handler {
		return otelhttp.NewHandler(next, serviceName,
			otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
				return r.Method + " " + r.URL.Path
			}),
		)
	}
}

// NewHTTPTransport returns an http.RoundTripper that traces outgoing HTTP requests.
// Use this with http.Client to propagate trace context to downstream services.
func NewHTTPTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return otelhttp.NewTransport(base)
}

// NewHTTPTransportWithTLS returns a traced transport with TLS configuration.
func NewHTTPTransportWithTLS(tlsConfig *tls.Config) http.RoundTripper {
	base := &http.Transport{TLSClientConfig: tlsConfig}
	return otelhttp.NewTransport(base)
}

// NewHTTPClient returns an *http.Client configured with tracing.
// All requests made with this client will have trace context propagated.
func NewHTTPClient() *http.Client {
	return &http.Client{
		Transport: NewHTTPTransport(nil),
	}
}
