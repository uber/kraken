# Kraken Distributed Tracing - Design Review

**Author:** [Your Name]  
**Date:** January 6, 2026  
**Status:** Pending Review

---

## 1. Project Goals

### What We're Building
Add distributed tracing to Kraken to enable visibility into blob download/upload operations across services.

### In Scope
| Goal | Description |
|------|-------------|
| Trace blob downloads | Agent → Scheduler → Tracker request flow |
| Trace blob uploads | Client → Proxy → Origin → Storage Backend |
| Trace replication | Cross-cluster blob replication workflows |
| Uber integration | Use existing Jaeger/M3 infrastructure |
| Low overhead | <1% performance impact on critical paths |

### Out of Scope
- **P2P communication** - Dispatcher, peer connections, piece exchanges will NOT be traced
- Custom storage backends or visualization UIs

---

## 2. Tracing Library Analysis: Jaeger vs SigNoz

### Overview

| Aspect | Jaeger | SigNoz |
|--------|--------|--------|
| **Origin** | Uber (CNCF Graduated) | Open-source startup |
| **Protocol** | OTLP, Thrift, gRPC | OTLP |
| **Storage** | Cassandra, Elasticsearch, Kafka | ClickHouse |
| **Go SDK** | OpenTelemetry (official) | OpenTelemetry |

### Comparison

| Criterion | Jaeger | SigNoz | Winner |
|-----------|--------|--------|--------|
| **Uber Ecosystem Fit** | Native integration with M3, existing tooling | No integration, additional infra needed | **Jaeger** |
| **Performance** | Battle-tested at 100K+ services, ~500ns/span | Good, but less proven at scale | **Jaeger** |
| **OpenTelemetry Support** | Full native support | Full native support | Tie |
| **Operational Burden** | Already deployed at Uber | New system to maintain | **Jaeger** |
| **All-in-one Observability** | Traces only (metrics via M3) | Traces + Metrics + Logs unified | **SigNoz** |
| **Query Performance** | Degrades with volume | ClickHouse is fast | **SigNoz** |

### Recommendation

**Use Jaeger with OpenTelemetry SDK**

**Rationale:**
1. Already part of Uber's infrastructure - no new systems to deploy/maintain
2. Proven at Uber scale (handles 100K+ services)
3. OpenTelemetry SDK provides vendor flexibility if we ever need to switch
4. Lower operational overhead - leverage existing Jaeger collectors

**Implementation:**
```go
// OpenTelemetry SDK with Jaeger exporter
"go.opentelemetry.io/otel"
"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
```

---

## 3. Request Flow Schema

### 3.1 Blob Download Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                    BLOB DOWNLOAD - ACTUAL FLOW                   │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Docker Client                                                   │
│       │                                                          │
│       │ GET /v2/{repo}/blobs/{digest}                           │
│       ▼                                                          │
│  ┌─────────┐                                                     │
│  │  AGENT  │  SPAN: agent.download_blob ← TRACEABLE (HTTP)      │
│  │         │  └── namespace, digest                             │
│  └────┬────┘                                                     │
│       │                                                          │
│       │ Cache miss? → Scheduler                                  │
│       ▼                                                          │
│  ┌─────────┐     HTTP POST /announce/{infohash}    ┌─────────┐  │
│  │SCHEDULER│ ─────────────────────────────────────►│ TRACKER │  │
│  │         │ ◄─────────────────────────────────────│         │  │
│  └────┬────┘     Returns peer list (HTTP)          └─────────┘  │
│       │                                                          │
│       │   SPAN: announce.request ← TRACEABLE (HTTP)             │
│       │   SPAN: tracker.announce ← TRACEABLE (HTTP)             │
│       │                                                          │
│       ▼                                                          │
│  ┌───────────────────────────────────────┐                      │
│  │    PIECE EXCHANGE (NOT TRACED)        │                      │
│  │  Agent ↔ Peers/Origins via TCP+Proto  │                      │
│  │  (BitTorrent protocol, NOT HTTP)      │                      │
│  └───────────────────────────────────────┘                      │
│       │                                                          │
│       │ Downloaded to local cache                                │
│       ▼                                                          │
│  ┌─────────┐                                                     │
│  │  AGENT  │  SPAN: agent.download_blob.complete ← TRACEABLE    │
│  │  CACHE  │  └── size, duration                                │
│  └─────────┘                                                     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│        ORIGIN BACKEND FETCH (when Origin has cache miss)         │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────┐                                                     │
│  │ ORIGIN  │  SPAN: origin.blob_refresh ← TRACEABLE             │
│  └────┬────┘                                                     │
│       │ Fetch from storage backend                               │
│       ▼                                                          │
│  ┌───────────┐                                                   │
│  │  BACKEND  │  SPAN: backend.download ← TRACEABLE              │
│  │ (S3/GCS)  │  └── backend_type, bucket, duration_ms           │
│  └─────┬─────┘                                                   │
│        │ Write to Origin cache                                   │
│        ▼                                                         │
│  ┌───────────┐                                                   │
│  │  ORIGIN   │  SPAN: cache.write ← TRACEABLE                   │
│  │  CACHE    │  └── path, size                                  │
│  └───────────┘                                                   │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

**What's Traceable:**
- ✅ Agent HTTP entry point
- ✅ Agent → Tracker (HTTP announce)
- ✅ Tracker → Agent (HTTP response)
- ✅ Origin → Backend (S3/GCS fetch)
- ✅ Agent completion

**NOT Traceable (TCP+Protobuf, not HTTP):**
- ❌ Agent ↔ Peers piece exchange
- ❌ Agent ↔ Origin piece exchange

### 3.2 Blob Upload Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                      BLOB UPLOAD TRACE                           │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Docker Client                                                   │
│       │                                                          │
│       │ POST/PATCH/PUT upload flow                               │
│       ▼                                                          │
│  ┌─────────┐                                                     │
│  │  PROXY  │  SPAN: proxy.upload                                │
│  │         │  └── namespace, repo, upload_id                    │
│  └────┬────┘                                                     │
│       │                                                          │
│       │ Forward blob to origin                                   │
│       ▼                                                          │
│  ┌─────────┐                                                     │
│  │ ORIGIN  │  SPAN: origin.store_blob                           │
│  │         │  └── digest, size, replicate                       │
│  └────┬────┘                                                     │
│       │                                                          │
│       │ Upload to storage backend                                │
│       ▼                                                          │
│  ┌─────────┐                                                     │
│  │ BACKEND │  SPAN: backend.upload                              │
│  │ (S3/GCS)│  └── backend_type, bucket, duration                │
│  └─────────┘                                                     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 3.3 Span Summary Table

| Trace | Spans | What's Traced |
|-------|-------|---------------|
| Blob Download (Agent) | 4 spans | Entry → Announce (HTTP) → Tracker → Complete |
| Piece Exchange | - | NOT traced (TCP+Protobuf, not HTTP) |
| Origin Backend Fetch | 3 spans | blob_refresh → backend.download → cache.write |
| Blob Upload | 3 spans | Proxy → Origin → Backend |
| Replication | 3 spans | Origin → Origin (cross-cluster) |

---

## 4. Performance Overhead Mitigation Plan

### 4.1 Performance Budget

| Metric | Target | Acceptable Max |
|--------|--------|----------------|
| Latency impact | <0.5% | <1% |
| CPU overhead | <0.3% | <0.5% |
| Memory per trace | <5KB | <10KB |
| Span creation time | <500ns | <1µs |

### 4.2 Mitigation Strategies

#### Strategy 1: Sampling

**Default: 10% sampling rate** - Only 1 in 10 requests will be traced.

```yaml
tracing:
  sampling_rate: 0.1  # 10%
  
  # Path-based rules
  sampling_rules:
    - path: "/health"      → 0%    # Never trace
    - path: "/readiness"   → 0%    # Never trace
    - path: "/announce/*"  → 5%    # High-frequency, lower rate
    - path: "/blobs/*"     → 20%   # Important operations, higher rate
```

#### Strategy 2: Async Batched Export

Traces are batched and exported asynchronously - never blocking request processing.

```
Request Thread          Background Exporter
     │                        │
     │ create span (~500ns)   │
     │ ────────────────────►  │
     │                        │ (queue span)
     │ continue processing    │
     │                        │
     │                        │ every 5s: batch export
     │                        │ ─────────────────────► Jaeger
```

**Config:**
- Batch timeout: 5 seconds
- Max batch size: 512 spans
- Queue size: 2048 spans

#### Strategy 3: No P2P Tracing

The highest-volume operations (peer connections, piece exchanges) are explicitly excluded. This eliminates potentially millions of spans per day.

### 4.3 Overhead Validation Plan

Before production rollout:

| Test | Method | Success Criteria |
|------|--------|------------------|
| Microbenchmark | Benchmark span creation | <1µs per span |
| Load test | Compare with/without tracing | <1% latency P99 diff |
| Memory test | Profile under load | <5% memory increase |
| CPU test | Profile under load | <0.5% CPU increase |

---

## 5. Open Questions for Review

| # | Question | Need Answer From |
|---|----------|------------------|
| 1 | Is 10% default sampling rate acceptable? | Performance/SRE |
| 2 | Any additional endpoints to exclude from tracing? | Team |
| 3 | Jaeger collector endpoint/config for Kraken? | Infra |
| 4 | Trace retention policy requirements? | SRE |

---

## 6. Next Steps (Pending Approval)

1. **Week 2:** Implement `pkg/tracing` core package
2. **Week 3:** Integrate into Agent, Origin, Tracker, Proxy
3. **Week 4:** Performance validation and production rollout

---

**Requesting review by:** [Reviewer Names]  
**Review deadline:** January 13, 2026

