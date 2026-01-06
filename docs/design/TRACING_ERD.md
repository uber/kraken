# Kraken Distributed Tracing - Engineering Requirements Document

**Author:** Engineering Team  
**Date:** January 6, 2026  
**Status:** Draft  
**Deadline:** Week 1 (January 13, 2026) - Analysis & Design Complete

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Goals and Non-Goals](#goals-and-non-goals)
3. [Background](#background)
4. [Tracing Library Comparative Analysis](#tracing-library-comparative-analysis)
5. [Request Flow Schema](#request-flow-schema)
6. [Technical Design](#technical-design)
7. [Performance Overhead Mitigation](#performance-overhead-mitigation)
8. [Implementation Plan](#implementation-plan)
9. [Success Metrics](#success-metrics)
10. [Risks and Mitigations](#risks-and-mitigations)
11. [Open Questions](#open-questions)
12. [References](#references)

---

## 1. Executive Summary

This document outlines the design for implementing distributed tracing in the Kraken P2P Docker registry system. Kraken currently relies on metrics (via tally/M3) and structured logging (via zap) for observability, but lacks end-to-end request tracing across its distributed components. This project aims to add distributed tracing capabilities to enable:

- End-to-end visibility of blob download/upload operations
- Performance bottleneck identification in service-to-service communication
- Improved debugging of cross-component failures
- Operational insights into cluster behavior

**Scope Note:** P2P communication (dispatcher, peer connections, piece exchanges) is explicitly **out of scope** for this project. Tracing will cover HTTP API endpoints and service-to-service calls only.

---

## 2. Goals and Non-Goals

### Goals

| ID | Goal | Priority |
|----|------|----------|
| G1 | Enable tracing of blob download requests (Agent → Scheduler → Tracker) | P0 |
| G2 | Enable tracing of blob upload operations (Client → Proxy → Origin → Backend) | P0 |
| G3 | Trace cross-cluster replication workflows | P1 |
| G4 | Integrate with Uber's existing observability infrastructure (M3/Jaeger) | P0 |
| G5 | Maintain <1% performance overhead on critical paths | P0 |
| G6 | Support trace correlation with existing logs and metrics | P1 |
| G7 | Enable sampling strategies for high-volume operations | P0 |

### Non-Goals

| ID | Non-Goal | Rationale |
|----|----------|-----------|
| NG1 | **Tracing P2P communication** | Explicit requirement - P2P layer is out of scope |
| NG2 | Tracing individual piece exchanges | Too granular; would create excessive overhead |
| NG3 | Tracing peer connections or dispatcher internals | Part of P2P layer - excluded |
| NG4 | Real-time trace visualization UI | Will use existing Jaeger/SigNoz UIs |
| NG5 | Custom trace storage backend | Leverage existing infrastructure |
| NG6 | Tracing internal goroutine scheduling | Not actionable; adds noise |

---

## 3. Background

### 3.1 Kraken Architecture Overview

Kraken is a P2P-powered Docker registry designed for scalability and availability in hybrid cloud environments. The system consists of five main components:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              KRAKEN ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌─────────┐         ┌─────────┐         ┌─────────┐                       │
│   │  Agent  │◄───────►│ Tracker │◄───────►│ Origin  │                       │
│   │ (Host)  │         │         │         │(Seeder) │                       │
│   └────┬────┘         └─────────┘         └────┬────┘                       │
│        │                                       │                             │
│        │              ┌─────────┐              │                             │
│        │              │  Proxy  │              │                             │
│        │              └────┬────┘              │                             │
│        │                   │                   │                             │
│        │              ┌────┴────┐              │                             │
│        └──────────────┤  Build  │◄─────────────┘                             │
│                       │  Index  │                                            │
│                       └─────────┘                                            │
│                                                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        STORAGE BACKENDS                              │   │
│   │            (S3, GCS, ECR, HDFS, Other Registries)                   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Current Observability State

| Aspect | Current Implementation | Gap |
|--------|----------------------|-----|
| **Metrics** | tally with M3/statsd backends | ✓ Good coverage |
| **Logging** | zap structured logging | ✓ Good coverage |
| **Tracing** | None | ✗ No distributed tracing |
| **Context Propagation** | Limited `context.Context` usage | ✗ Not propagated across services |

### 3.3 Scale Characteristics

Understanding scale is critical for tracing design:

- **Daily Volume**: 1M+ blobs distributed
- **Peak Load**: 20K blobs (100MB-1G) in 30 seconds
- **Cluster Size**: Up to 15K hosts per cluster
- **Blob Sizes**: Up to 20GB (typically 100MB-1G)

---

## 4. Tracing Library Comparative Analysis

### 4.1 Evaluation Criteria

| Criterion | Weight | Description |
|-----------|--------|-------------|
| **Uber Ecosystem Compatibility** | 30% | Integration with M3, Jaeger, existing tooling |
| **Performance Overhead** | 25% | CPU, memory, and latency impact |
| **OpenTelemetry Compatibility** | 20% | Future-proofing, industry standard |
| **Ease of Integration** | 15% | Go library quality, learning curve |
| **Community & Support** | 10% | Active development, documentation |

### 4.2 Candidate Analysis

#### 4.2.1 Jaeger (with OpenTelemetry SDK)

| Aspect | Details |
|--------|---------|
| **Overview** | Uber-originated distributed tracing system, now CNCF graduated |
| **Architecture** | Agent → Collector → Storage (Cassandra/ES/Kafka) → Query Service |
| **Go SDK** | `go.opentelemetry.io/otel` with Jaeger exporter |
| **Sampling** | Adaptive, probabilistic, rate-limiting, remote |
| **Protocol** | Native Thrift, gRPC, HTTP (OTLP) |

**Pros:**
- ✅ Native Uber integration (M3, internal tooling)
- ✅ Battle-tested at Uber scale (100K+ services)
- ✅ Full OpenTelemetry compatibility
- ✅ Excellent Go SDK with context propagation
- ✅ Low overhead with proper sampling (~0.1-0.5% CPU)

**Cons:**
- ⚠️ Requires infrastructure (collector, storage)
- ⚠️ Query performance degrades with volume

**Performance Characteristics:**
```
Span Creation:      ~500ns/span
Context Injection:  ~200ns/request
Memory per Span:    ~1KB
Export Overhead:    Batched, configurable
```

#### 4.2.2 SigNoz

| Aspect | Details |
|--------|---------|
| **Overview** | Open-source APM with traces, metrics, and logs in one platform |
| **Architecture** | OTLP receiver → ClickHouse → Query service |
| **Go SDK** | Standard OpenTelemetry SDK |
| **Sampling** | OpenTelemetry sampling strategies |
| **Protocol** | OTLP (gRPC/HTTP) |

**Pros:**
- ✅ All-in-one observability (metrics + traces + logs)
- ✅ ClickHouse provides fast queries
- ✅ Native OpenTelemetry support
- ✅ Self-hosted option
- ✅ Modern UI with good UX

**Cons:**
- ❌ Not integrated with Uber's existing infrastructure
- ❌ Smaller community than Jaeger
- ⚠️ Additional operational burden
- ⚠️ Less mature than Jaeger

#### 4.2.3 OpenTelemetry (Vendor-Agnostic)

| Aspect | Details |
|--------|---------|
| **Overview** | Vendor-neutral observability framework (CNCF) |
| **Architecture** | SDK → Collector → Any backend |
| **Go SDK** | `go.opentelemetry.io/otel` |
| **Sampling** | Parent-based, trace-id ratio, always-on/off |
| **Protocol** | OTLP (native), adapters for others |

**Pros:**
- ✅ Vendor-neutral, future-proof
- ✅ Supports multiple exporters (Jaeger, Zipkin, OTLP)
- ✅ Active CNCF development
- ✅ Unified API for traces, metrics, logs
- ✅ Collector provides processing pipeline

**Cons:**
- ⚠️ More complex setup than single-vendor solutions
- ⚠️ Requires choosing exporters/backends

#### 4.2.4 Zipkin

| Aspect | Details |
|--------|---------|
| **Overview** | Twitter-originated distributed tracing system |
| **Architecture** | Direct report or via collector → Storage → UI |
| **Go SDK** | `github.com/openzipkin/zipkin-go` |

**Pros:**
- ✅ Simple architecture
- ✅ Lightweight
- ✅ Good B3 propagation support

**Cons:**
- ❌ Not part of Uber ecosystem
- ❌ Less feature-rich than Jaeger
- ❌ Smaller Go community

### 4.3 Comparison Matrix

| Feature | Jaeger + OTel | SigNoz | Pure OTel | Zipkin |
|---------|---------------|--------|-----------|--------|
| **Uber Integration** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ |
| **Performance** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| **OTel Compat** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Ease of Integration** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Community** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Total (Weighted)** | **4.65** | **3.35** | **4.30** | **3.30** |

### 4.4 Recommendation

**Primary Choice: OpenTelemetry SDK with Jaeger Exporter**

**Rationale:**
1. **Uber Ecosystem Alignment**: Jaeger is Uber-originated and deeply integrated with internal tooling
2. **OpenTelemetry Future-Proofing**: Using OTel SDK allows swapping backends without code changes
3. **Proven Scale**: Jaeger handles Uber's 100K+ service mesh
4. **Performance**: Sub-microsecond span creation with efficient batching
5. **Sampling Flexibility**: Adaptive sampling critical for Kraken's variable load

**Implementation Stack:**
```go
// SDK: OpenTelemetry Go
"go.opentelemetry.io/otel"
"go.opentelemetry.io/otel/sdk/trace"
"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"

// Propagation: W3C Trace Context + Jaeger
"go.opentelemetry.io/otel/propagation"

// Exporter: OTLP to Jaeger Collector
"go.opentelemetry.io/otel/exporters/jaeger"
```

---

## 5. Request Flow Schema

### 5.1 Blob Download Flow (P2P Path)

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           BLOB DOWNLOAD TRACE FLOW                                   │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  Docker Client                                                                       │
│       │                                                                              │
│       │ GET /v2/{repo}/blobs/{digest}                                               │
│       ▼                                                                              │
│  ┌─────────┐  SPAN: agent.download_blob                                             │
│  │  Agent  │  ├── namespace: string                                                 │
│  │ Server  │  ├── digest: string                                                    │
│  └────┬────┘  └── size: int64                                                       │
│       │                                                                              │
│       │ Cache Miss                                                                   │
│       ▼                                                                              │
│  ┌──────────┐  SPAN: scheduler.download                                             │
│  │Scheduler │  ├── torrent.infohash: string                                         │
│  │          │  ├── torrent.size: int64                                              │
│  └────┬─────┘  └── peer.id: string                                                  │
│       │                                                                              │
│       │ (1) Create Torrent                                                          │
│       ▼                                                                              │
│  ┌──────────┐  SPAN: storage.create_torrent                                         │
│  │ Torrent  │  ├── pieces: int                                                      │
│  │ Archive  │  └── piece_length: int                                                │
│  └────┬─────┘                                                                        │
│       │                                                                              │
│       │ (2) Announce to Tracker                                                      │
│       ▼                                                                              │
│  ┌──────────┐  SPAN: announce.request [CROSS-SERVICE]                               │
│  │ Announce │  ├── tracker.addr: string                                             │
│  │  Client  │  ├── infohash: string                                                 │
│  └────┬─────┘  └── complete: bool                                                   │
│       │                                                                              │
│       │ HTTP POST /announce/{infohash}                                              │
│       ▼                                                                              │
│  ┌──────────┐  SPAN: tracker.announce [SERVER]                                      │
│  │ Tracker  │  ├── peer_count: int                                                  │
│  │ Server   │  ├── origin_count: int                                                │
│  └────┬─────┘  └── handout_policy: string                                           │
│       │                                                                              │
│       │ Returns peer list                                                            │
│       │                                                                              │
│  ┌──────────────────────────────────────────────┐                                   │
│  │         P2P LAYER (NOT TRACED)               │                                   │
│  │                                              │                                   │
│  │  Dispatcher, peer connections, piece         │                                   │
│  │  exchanges are OUT OF SCOPE per requirements │                                   │
│  │                                              │                                   │
│  │  • No spans for peer.connection              │                                   │
│  │  • No spans for dispatcher operations        │                                   │
│  │  • No spans for piece requests/responses     │                                   │
│  └──────────────────────────────────────────────┘                                   │
│       │                                                                              │
│       │ (3) Download Complete                                                        │
│       ▼                                                                              │
│  ┌──────────┐  SPAN: storage.commit                                                 │
│  │  Cache   │  ├── verified: bool                                                   │
│  │  Store   │  └── cache_hit: bool                                                  │
│  └──────────┘                                                                        │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Blob Upload Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                            BLOB UPLOAD TRACE FLOW                                    │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  Docker Client                                                                       │
│       │                                                                              │
│       │ POST /v2/{repo}/blobs/uploads/                                              │
│       ▼                                                                              │
│  ┌─────────┐  SPAN: proxy.start_upload                                              │
│  │  Proxy  │  ├── namespace: string                                                 │
│  │ Server  │  ├── repo: string                                                      │
│  └────┬────┘  └── upload_id: string                                                 │
│       │                                                                              │
│       │ PATCH (chunk uploads)                                                        │
│       ▼                                                                              │
│  ┌─────────┐  SPAN: proxy.upload_chunk                                              │
│  │  Proxy  │  ├── range_start: int64                                                │
│  │         │  ├── range_end: int64                                                  │
│  └────┬────┘  └── chunk_size: int64                                                 │
│       │                                                                              │
│       │ PUT (commit upload)                                                          │
│       ▼                                                                              │
│  ┌─────────┐  SPAN: proxy.commit_upload                                             │
│  │  Proxy  │  ├── digest: string                                                    │
│  │         │  └── total_size: int64                                                 │
│  └────┬────┘                                                                         │
│       │                                                                              │
│       │ Forward to Origin                                                            │
│       ▼                                                                              │
│  ┌─────────┐  SPAN: origin.store_blob [CROSS-SERVICE]                               │
│  │ Origin  │  ├── namespace: string                                                 │
│  │ Server  │  ├── digest: string                                                    │
│  └────┬────┘  └── replicate: bool                                                   │
│       │                                                                              │
│       │ Upload to Backend                                                            │
│       ▼                                                                              │
│  ┌─────────┐  SPAN: backend.upload                                                  │
│  │ Storage │  ├── backend_type: string (s3/gcs/hdfs)                                │
│  │ Backend │  ├── bucket: string                                                    │
│  └─────────┘  └── duration_ms: int64                                                │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 5.3 Cross-Cluster Replication Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        CROSS-CLUSTER REPLICATION TRACE                               │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  ┌─────────────┐                              ┌─────────────┐                       │
│  │ Source      │                              │ Destination │                       │
│  │ Cluster     │                              │ Cluster     │                       │
│  └──────┬──────┘                              └──────┬──────┘                       │
│         │                                            │                              │
│  SPAN: replication.trigger                           │                              │
│  ├── tag: string                                     │                              │
│  ├── src_cluster: string                             │                              │
│  └── dst_cluster: string                             │                              │
│         │                                            │                              │
│         │ (1) Resolve tag to digest                  │                              │
│         ▼                                            │                              │
│  SPAN: build_index.resolve_tag                       │                              │
│  └── digest: string                                  │                              │
│         │                                            │                              │
│         │ (2) Check existence on destination         │                              │
│         ├────────────────────────────────────────────►                              │
│         │ SPAN: origin.stat_blob [CROSS-CLUSTER]     │                              │
│         │ └── exists: bool                           │                              │
│         │                                            │                              │
│         │ (3) Transfer blob if not exists            │                              │
│         ├────────────────────────────────────────────►                              │
│         │ SPAN: replication.transfer                 │                              │
│         │ ├── size: int64                            │                              │
│         │ └── duration_ms: int64                     │                              │
│         │                                            │                              │
│         │ (4) Update destination build-index         │                              │
│         ├────────────────────────────────────────────►                              │
│         │ SPAN: build_index.put_tag                  │                              │
│         │                                            │                              │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 5.4 Span Hierarchy Summary

```
Trace: blob.download
├── agent.download_blob (root)
│   ├── agent.cache_lookup
│   ├── scheduler.download
│   │   ├── storage.create_torrent
│   │   ├── announce.request → [Tracker Service]
│   │   │   └── tracker.announce
│   │   │       ├── peerstore.get_peers
│   │   │       └── policy.sort_peers
│   │   │
│   │   │   [P2P LAYER - NOT TRACED]
│   │   │   (dispatcher, peer connections, piece exchanges)
│   │   │
│   │   └── storage.commit
│   └── agent.stream_response

Trace: blob.upload
├── proxy.upload (root)
│   ├── proxy.start_upload
│   ├── proxy.upload_chunk (per chunk)
│   ├── proxy.commit_upload
│   │   └── origin.store_blob → [Origin Service]
│   │       ├── origin.cache_write
│   │       └── backend.upload
│   └── build_index.put_tag → [Build-Index Service]
```

**Note:** P2P communication tracing is explicitly out of scope. The trace ends at the announce request and resumes at storage commit.

---

## 6. Technical Design

### 6.1 Trace Provider Initialization

```go
// pkg/tracing/provider.go

package tracing

import (
    "context"
    "time"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/sdk/resource"
    "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// Config holds tracing configuration
type Config struct {
    Enabled         bool          `yaml:"enabled"`
    ServiceName     string        `yaml:"service_name"`
    CollectorAddr   string        `yaml:"collector_addr"`
    SamplingRate    float64       `yaml:"sampling_rate"`    // 0.0 - 1.0
    BatchTimeout    time.Duration `yaml:"batch_timeout"`
    MaxExportBatch  int           `yaml:"max_export_batch"`
    MaxQueueSize    int           `yaml:"max_queue_size"`
}

func (c Config) applyDefaults() Config {
    if c.SamplingRate == 0 {
        c.SamplingRate = 0.1 // 10% default
    }
    if c.BatchTimeout == 0 {
        c.BatchTimeout = 5 * time.Second
    }
    if c.MaxExportBatch == 0 {
        c.MaxExportBatch = 512
    }
    if c.MaxQueueSize == 0 {
        c.MaxQueueSize = 2048
    }
    return c
}

// InitProvider initializes the OpenTelemetry trace provider
func InitProvider(ctx context.Context, cfg Config) (func(context.Context) error, error) {
    cfg = cfg.applyDefaults()
    
    if !cfg.Enabled {
        // Return no-op provider
        otel.SetTracerProvider(trace.NewTracerProvider())
        return func(ctx context.Context) error { return nil }, nil
    }

    // Create OTLP exporter
    exporter, err := otlptracegrpc.New(ctx,
        otlptracegrpc.WithEndpoint(cfg.CollectorAddr),
        otlptracegrpc.WithInsecure(), // Configure TLS in production
    )
    if err != nil {
        return nil, err
    }

    // Create resource with service info
    res, err := resource.Merge(
        resource.Default(),
        resource.NewWithAttributes(
            semconv.SchemaURL,
            semconv.ServiceName(cfg.ServiceName),
            semconv.ServiceVersion(version.GitDescribe),
        ),
    )
    if err != nil {
        return nil, err
    }

    // Create sampler
    sampler := trace.ParentBased(
        trace.TraceIDRatioBased(cfg.SamplingRate),
    )

    // Create trace provider
    tp := trace.NewTracerProvider(
        trace.WithBatcher(exporter,
            trace.WithBatchTimeout(cfg.BatchTimeout),
            trace.WithMaxExportBatchSize(cfg.MaxExportBatch),
            trace.WithMaxQueueSize(cfg.MaxQueueSize),
        ),
        trace.WithResource(res),
        trace.WithSampler(sampler),
    )

    // Set global provider
    otel.SetTracerProvider(tp)

    // Set propagator (W3C Trace Context + Baggage)
    otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
        propagation.TraceContext{},
        propagation.Baggage{},
    ))

    return tp.Shutdown, nil
}
```

### 6.2 HTTP Middleware for Trace Propagation

```go
// pkg/tracing/middleware.go

package tracing

import (
    "net/http"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/propagation"
    semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
    "go.opentelemetry.io/otel/trace"
)

// HTTPServerMiddleware creates spans for incoming HTTP requests
func HTTPServerMiddleware(serviceName string) func(http.Handler) http.Handler {
    tracer := otel.Tracer(serviceName)
    
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            // Extract trace context from incoming request
            ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

            // Start span
            spanName := r.Method + " " + r.URL.Path
            ctx, span := tracer.Start(ctx, spanName,
                trace.WithSpanKind(trace.SpanKindServer),
                trace.WithAttributes(
                    semconv.HTTPMethod(r.Method),
                    semconv.HTTPURL(r.URL.String()),
                    semconv.HTTPScheme(r.URL.Scheme),
                    semconv.NetHostName(r.Host),
                ),
            )
            defer span.End()

            // Wrap response writer to capture status code
            rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

            // Pass context with span to next handler
            next.ServeHTTP(rw, r.WithContext(ctx))

            // Record status
            span.SetAttributes(semconv.HTTPStatusCode(rw.statusCode))
            if rw.statusCode >= 400 {
                span.SetStatus(codes.Error, http.StatusText(rw.statusCode))
            }
        })
    }
}

// HTTPClientTransport wraps http.RoundTripper for outgoing requests
type HTTPClientTransport struct {
    Base http.RoundTripper
}

func (t *HTTPClientTransport) RoundTrip(r *http.Request) (*http.Response, error) {
    tracer := otel.Tracer("http-client")
    
    ctx, span := tracer.Start(r.Context(), r.Method+" "+r.URL.Host+r.URL.Path,
        trace.WithSpanKind(trace.SpanKindClient),
        trace.WithAttributes(
            semconv.HTTPMethod(r.Method),
            semconv.HTTPURL(r.URL.String()),
        ),
    )
    defer span.End()

    // Inject trace context into outgoing request
    otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(r.Header))

    resp, err := t.Base.RoundTrip(r.WithContext(ctx))
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, err.Error())
        return nil, err
    }

    span.SetAttributes(semconv.HTTPStatusCode(resp.StatusCode))
    return resp, nil
}
```

### 6.3 Context Propagation for Internal Operations

```go
// pkg/tracing/context.go

package tracing

import (
    "context"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/codes"
    "go.opentelemetry.io/otel/trace"
)

// StartSpan starts a new span as a child of any span in ctx
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
    tracer := otel.Tracer("kraken")
    return tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

// SpanFromContext returns the current span from context
func SpanFromContext(ctx context.Context) trace.Span {
    return trace.SpanFromContext(ctx)
}

// RecordError records an error on the current span
func RecordError(ctx context.Context, err error) {
    span := trace.SpanFromContext(ctx)
    span.RecordError(err)
    span.SetStatus(codes.Error, err.Error())
}

// AddEvent adds an event to the current span
func AddEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
    span := trace.SpanFromContext(ctx)
    span.AddEvent(name, trace.WithAttributes(attrs...))
}

// SetAttributes sets attributes on the current span
func SetAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
    span := trace.SpanFromContext(ctx)
    span.SetAttributes(attrs...)
}
```

### 6.4 Integration Points

#### 6.4.1 Agent Server Integration

```go
// agent/agentserver/server.go (modified)

func (s *Server) Handler() http.Handler {
    r := chi.NewRouter()

    // Add tracing middleware FIRST
    r.Use(tracing.HTTPServerMiddleware("kraken-agent"))
    
    r.Use(middleware.StatusCounter(s.stats))
    r.Use(middleware.LatencyTimer(s.stats))

    // ... existing routes ...
}

func (s *Server) downloadBlobHandler(w http.ResponseWriter, r *http.Request) error {
    ctx := r.Context()
    ctx, span := tracing.StartSpan(ctx, "agent.download_blob",
        attribute.String("namespace", namespace),
        attribute.String("digest", d.Hex()),
    )
    defer span.End()

    // ... existing logic with ctx passed through ...
}
```

#### 6.4.2 Scheduler Integration

```go
// lib/torrent/scheduler/scheduler.go (modified)

func (s *scheduler) Download(ctx context.Context, namespace string, d core.Digest) error {
    ctx, span := tracing.StartSpan(ctx, "scheduler.download",
        attribute.String("namespace", namespace),
        attribute.String("digest", d.Hex()),
    )
    defer span.End()

    start := time.Now()
    size, err := s.doDownload(ctx, namespace, d)
    
    span.SetAttributes(
        attribute.Int64("size_bytes", size),
        attribute.Int64("duration_ms", time.Since(start).Milliseconds()),
    )
    
    if err != nil {
        tracing.RecordError(ctx, err)
    }
    return err
}
```

### 6.5 Configuration Schema

```yaml
# config/agent/base.yaml (additions)

tracing:
  enabled: true
  service_name: kraken-agent
  collector_addr: jaeger-collector:4317
  sampling_rate: 0.1  # 10% sampling
  batch_timeout: 5s
  max_export_batch: 512
  max_queue_size: 2048
  
  # Dynamic sampling rules
  sampling_rules:
    - path: "/health"
      rate: 0.0  # Never trace health checks
    - path: "/announce/*"
      rate: 0.05  # Lower rate for frequent announces
    - path: "/blobs/*"
      rate: 0.2   # Higher rate for blob operations
```

---

## 7. Performance Overhead Mitigation

### 7.1 Performance Budget

| Component | Maximum Overhead | Measurement Method |
|-----------|------------------|-------------------|
| Span Creation | <1µs | Benchmark |
| Context Propagation | <500ns | Benchmark |
| HTTP Header Injection | <200ns | Benchmark |
| Memory per Trace | <10KB | Profiling |
| Network (export) | <1% bandwidth | Monitoring |
| **Total Latency Impact** | **<1%** | End-to-end |

### 7.2 Mitigation Strategies

#### 7.2.1 Intelligent Sampling

```go
// pkg/tracing/sampler.go

// AdaptiveSampler adjusts sampling based on load
type AdaptiveSampler struct {
    baseSampler   trace.Sampler
    rateLimiter   *rate.Limiter
    loadThreshold float64
    lowLoadRate   float64
    highLoadRate  float64
}

func (s *AdaptiveSampler) ShouldSample(p trace.SamplingParameters) trace.SamplingResult {
    // Check current load (could be from metrics)
    currentLoad := getCurrentSystemLoad()
    
    var effectiveRate float64
    if currentLoad > s.loadThreshold {
        effectiveRate = s.highLoadRate  // Lower sampling under high load
    } else {
        effectiveRate = s.lowLoadRate
    }
    
    // Apply rate limiting
    if !s.rateLimiter.Allow() {
        return trace.SamplingResult{Decision: trace.Drop}
    }
    
    return s.baseSampler.ShouldSample(p)
}
```

#### 7.2.2 Async Export with Backpressure

```go
// Configured in trace provider initialization
trace.WithBatcher(exporter,
    trace.WithBatchTimeout(5 * time.Second),
    trace.WithMaxExportBatchSize(512),
    trace.WithMaxQueueSize(2048),
    trace.WithBlocking(), // Block on queue full instead of dropping
)
```

#### 7.2.3 Path-Based Sampling Exclusions

```go
// Exclude high-frequency, low-value paths
samplingRules := map[string]float64{
    "/health":     0.0,   // Never trace
    "/readiness":  0.0,   // Never trace  
    "/metrics":    0.0,   // Never trace
    "/debug/*":    0.01,  // Very low rate
}
```

### 7.3 Overhead Monitoring

```go
// Built-in metrics for tracing overhead
type TracingMetrics struct {
    SpansCreated    tally.Counter
    SpansDropped    tally.Counter
    ExportLatency   tally.Timer
    ExportErrors    tally.Counter
    QueueSize       tally.Gauge
    SamplingRate    tally.Gauge
}
```

### 7.4 Circuit Breaker for Tracing

```go
// Disable tracing under extreme load
type TracingCircuitBreaker struct {
    enabled       atomic.Bool
    loadThreshold float64
    cooldown      time.Duration
}

func (cb *TracingCircuitBreaker) ShouldTrace() bool {
    if !cb.enabled.Load() {
        return false
    }
    
    currentLoad := getCurrentSystemLoad()
    if currentLoad > cb.loadThreshold {
        cb.enabled.Store(false)
        go func() {
            time.Sleep(cb.cooldown)
            cb.enabled.Store(true)
        }()
        return false
    }
    return true
}
```

---

## 8. Implementation Plan

### 8.1 Phase Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           IMPLEMENTATION TIMELINE                                    │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  Week 1 (Jan 6-13)     Week 2 (Jan 13-20)    Week 3 (Jan 20-27)    Week 4 (Jan 27+) │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌────────────┐   │
│  │ Analysis &      │   │ Core            │   │ Integration     │   │ Rollout &  │   │
│  │ Design (ERD)    │   │ Implementation  │   │ & Testing       │   │ Tuning     │   │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘   └────────────┘   │
│                                                                                      │
│  Deliverables:          Deliverables:         Deliverables:        Deliverables:    │
│  - This ERD             - pkg/tracing         - Agent tracing      - Prod rollout   │
│  - Library analysis     - HTTP middleware     - Origin tracing     - Sampling tune  │
│  - Request flow schema  - Config schema       - Tracker tracing    - Dashboard      │
│  - Perf mitigation      - Unit tests          - Integration tests  - Runbook        │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 Detailed Tasks

#### Week 1: Analysis & Design ✓
- [x] Analyze Kraken codebase architecture
- [x] Evaluate tracing libraries
- [x] Document request flow schemas
- [x] Design performance mitigation plan
- [x] Create this ERD document

#### Week 2: Core Implementation
- [ ] Create `pkg/tracing` package
- [ ] Implement trace provider initialization
- [ ] Implement HTTP middleware (server + client)
- [ ] Implement context propagation helpers
- [ ] Add configuration schema
- [ ] Unit tests for tracing package
- [ ] Benchmarks for overhead validation

#### Week 3: Integration & Testing
- [ ] Integrate tracing into Agent server
- [ ] Integrate tracing into Origin server
- [ ] Integrate tracing into Tracker server
- [ ] Integrate tracing into Proxy server
- [ ] Integrate tracing into Build-Index server
- [ ] Add tracing to scheduler (excluding P2P layer)
- [ ] Integration tests with Jaeger
- [ ] Performance testing under load

#### Week 4: Rollout & Tuning
- [ ] Deploy to staging cluster
- [ ] Tune sampling rates
- [ ] Create Jaeger dashboards
- [ ] Document operational runbook
- [ ] Gradual production rollout
- [ ] Monitor overhead metrics

---

## 9. Success Metrics

### 9.1 Technical Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| P99 Latency Impact | <1% | A/B comparison |
| CPU Overhead | <0.5% | Profiling |
| Memory Overhead | <5% | Profiling |
| Trace Completeness | >95% | Sampling validation |
| Export Success Rate | >99.9% | Exporter metrics |

### 9.2 Operational Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| MTTD (Mean Time to Detect) | -30% | Incident tracking |
| MTTR (Mean Time to Resolve) | -25% | Incident tracking |
| Cross-service visibility | 100% | Trace coverage |
| Debug session duration | -40% | Developer survey |

### 9.3 Adoption Metrics

| Metric | Target | Timeline |
|--------|--------|----------|
| Trace query usage | >50 queries/day | Week 6 |
| Dashboard active users | >10 | Week 8 |
| Documented troubleshooting guides | 5+ | Week 8 |

---

## 10. Risks and Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Performance degradation under peak load | Medium | High | Adaptive sampling, circuit breaker, load testing |
| Trace data volume exceeds storage | Low | Medium | Aggressive sampling, retention policies |
| Context propagation breaks existing code | Low | High | Comprehensive testing, gradual rollout |
| Jaeger collector becomes bottleneck | Low | Medium | Collector scaling, async export with buffering |
| Developer adoption resistance | Medium | Medium | Documentation, training, showcase value |

---

## 11. Open Questions

| # | Question | Owner | Due Date | Status |
|---|----------|-------|----------|--------|
| 1 | What sampling rate balances visibility vs. overhead for peak load? | Performance Team | Week 3 | Open |
| 2 | Should we trace P2P communication? | Arch Review | Week 1 | **Resolved: No - P2P layer is out of scope** |
| 3 | What retention policy for trace data? | SRE Team | Week 3 | Open |
| 4 | Integration with existing M3 metrics correlation? | Observability Team | Week 3 | Open |

---

## 12. References

1. [OpenTelemetry Go Documentation](https://opentelemetry.io/docs/instrumentation/go/)
2. [Jaeger Architecture](https://www.jaegertracing.io/docs/architecture/)
3. [Uber Engineering Blog: Distributed Tracing at Uber](https://eng.uber.com/distributed-tracing/)
4. [W3C Trace Context Specification](https://www.w3.org/TR/trace-context/)
5. [Kraken Architecture Documentation](./ARCHITECTURE.md)
6. [OpenTelemetry Sampling](https://opentelemetry.io/docs/concepts/sampling/)

---

## Appendix A: Go Module Dependencies

```go
// Required additions to go.mod
require (
    go.opentelemetry.io/otel v1.21.0
    go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.21.0
    go.opentelemetry.io/otel/sdk v1.21.0
    go.opentelemetry.io/otel/trace v1.21.0
    go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.46.0
)
```

## Appendix B: Configuration Example

```yaml
# Complete tracing configuration example
tracing:
  enabled: true
  service_name: kraken-agent
  collector_addr: jaeger-collector.observability.svc:4317
  
  # Sampling configuration
  sampling:
    default_rate: 0.1
    rules:
      - match:
          path_prefix: "/health"
        rate: 0.0
      - match:
          path_prefix: "/namespace/*/blobs/*"
        rate: 0.2
      - match:
          operation: "scheduler.download"
        rate: 0.5
  
  # Export configuration  
  export:
    batch_timeout: 5s
    max_batch_size: 512
    max_queue_size: 2048
    retry_enabled: true
    retry_max_attempts: 3
  
  # Performance safeguards
  safeguards:
    circuit_breaker_enabled: true
    load_threshold: 0.9
    cooldown_duration: 30s
```

---

**Document History:**

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-06 | Engineering Team | Initial draft |

**Approvals:**

| Role | Name | Date | Status |
|------|------|------|--------|
| Tech Lead | | | Pending |
| Architect | | | Pending |
| SRE | | | Pending |

