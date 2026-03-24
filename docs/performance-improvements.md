# Plan: Improve Kraken Agent Network Throughput

## Context

Kraken's Agent uses a custom P2P protocol (BitTorrent-inspired) to download blobs from peers. Several default configuration values and implementation details limit throughput below what modern network hardware supports. This plan targets the highest-impact, lowest-risk improvements that can be validated with unit tests and deployed incrementally.

**Excluded from scope** (too invasive or not justified):
- Eliminating in-memory piece payload buffering — requires changing the `storage.Torrent` interface (`WritePiece` needs `src.Length()` synchronously), touching `agentstorage`, `originstorage`, and all related tests. Memory cost is bounded by PipelineLimit × piece size — manageable.
- Backpressure/timeout on `conn.Send()` — the non-blocking drop is a deliberate design choice (see TODO comment at `conn.go:163`). Increasing `SenderBufferSize` via config is safer if drops become observable.

---

## Changes (in implementation order)

### 1. Increase `PipelineLimit` default: 3 → 8
**File**: `lib/torrent/scheduler/dispatch/config.go:62`

Only 3 concurrent piece requests per peer under-utilizes high-throughput or high-latency links. With 10 peers and 3 requests each, the agent has at most 30 in-flight pieces at any time. Increasing to 8 triples in-flight pieces with no code changes beyond the default.

**Code change**:
```go
// Before
if c.PipelineLimit == 0 {
    c.PipelineLimit = 3
}
// After
if c.PipelineLimit == 0 {
    c.PipelineLimit = 8
}
```

Also verify `EndgameThreshold` coupling — currently defaults to `PipelineLimit` value. Confirm endgame threshold still reasonable (can stay at 3 explicitly).

**Tests**: Update any tests that hardcode `PipelineLimit=3` to use explicit values. Add `TestConfigDefaults` that asserts the new default.

---

### 2. Increase `MaxOpenConnectionsPerTorrent` default: 10 → 20
**File**: `lib/torrent/scheduler/connstate/config.go:39`

Limits parallelism to 10 peers per torrent. For large blobs with many seeders, this leaves bandwidth on the table. Doubling to 20 peers doubles potential download sources.

**Code change**:
```go
// Before
if c.MaxOpenConnectionsPerTorrent == 0 {
    c.MaxOpenConnectionsPerTorrent = 10
}
// After
if c.MaxOpenConnectionsPerTorrent == 0 {
    c.MaxOpenConnectionsPerTorrent = 20
}
```

**Tests**: Update connstate tests that assert on max connections count.

---

### 3. Use `io.CopyBuffer` with 1 MB buffer in agent download handler
**File**: `agent/agentserver/server.go:163`

`io.Copy()` uses a 32 KB internal buffer. For large blob downloads, a 1 MB buffer reduces syscall overhead and improves streaming throughput by 10–20%.

**Code change**:
```go
// Before
if _, err := io.Copy(w, blob); err != nil { ... }

// After
buf := make([]byte, 1<<20) // 1 MB
if _, err := io.CopyBuffer(w, blob, buf); err != nil { ... }
```

**Tests**: Existing handler tests cover correctness. Add a benchmark comparing `io.Copy` vs `io.CopyBuffer` with 1 MB buffer on large payloads.

---

### 4. Context-aware bandwidth reservation (two sub-steps)

**Problem**: `bandwidth.Limiter.reserve()` calls `time.Sleep(r.Delay())` unconditionally. This blocks `readLoop`/`writeLoop` goroutines with no cancellation path. When a `Conn` is closed, goroutines may hang until the sleep completes.

#### Step 4a: Add `ReserveIngressCtx`/`ReserveEgressCtx` to bandwidth.Limiter
**File**: `utils/bandwidth/limiter.go`

```go
func (l *Limiter) ReserveIngressCtx(ctx context.Context, nbytes int64) error {
    return l.reserveCtx(ctx, l.ingress, nbytes)
}

func (l *Limiter) ReserveEgressCtx(ctx context.Context, nbytes int64) error {
    return l.reserveCtx(ctx, l.egress, nbytes)
}

func (l *Limiter) reserveCtx(ctx context.Context, rl *rate.Limiter, nbytes int64) error {
    if !l.config.Enable {
        return nil
    }
    tokens := int(uint64(nbytes*8) / l.config.TokenSize)
    if tokens == 0 {
        tokens = 1
    }
    r := rl.ReserveN(time.Now(), tokens)
    if !r.OK() {
        return errExceedsBurst
    }
    delay := r.Delay()
    if delay == 0 {
        return nil
    }
    t := time.NewTimer(delay)
    defer t.Stop()
    select {
    case <-t.C:
        return nil
    case <-ctx.Done():
        r.Cancel()
        return ctx.Err()
    }
}
```

**Tests**: `bandwidth/limiter_test.go` — add test canceling context mid-reservation, verify token is returned via `r.Cancel()` and no goroutine hangs.

#### Step 4b: Thread context through `Conn`
**File**: `lib/torrent/scheduler/conn/conn.go`

Add `ctx context.Context` and `ctxCancel context.CancelFunc` fields to `Conn`. Initialize in `newConn()`, call `ctxCancel()` in `Close()`. Pass `ctx` to `readPayload()` and `sendPiecePayload()` which call the new `ReserveIngressCtx`/`ReserveEgressCtx`.

**Tests**: `conn_test.go` — add test that closes a `Conn` while bandwidth reservation is in progress and verifies both goroutines exit promptly (no hang).

---

### 5. Custom HTTP transport with connection pool tuning
**File**: `utils/httputil/httputil.go`

`http.DefaultTransport` has `MaxIdleConnsPerHost=2`. With many concurrent blob downloads from origins, this causes excessive TCP connection churn. A custom transport with larger pool sizes reduces handshake overhead.

**Code change** — add `DefaultKrakenTransport()`:
```go
func DefaultKrakenTransport() http.RoundTripper {
    return &http.Transport{
        MaxIdleConns:        500,
        MaxIdleConnsPerHost: 50,
        MaxConnsPerHost:     100,
        IdleConnTimeout:     90 * time.Second,
        // Preserve TLS and proxy settings from stdlib defaults.
        TLSHandshakeTimeout:   10 * time.Second,
        ExpectContinueTimeout: 1 * time.Second,
    }
}
```

Wire into agent's origin blob client construction (the call site where `httputil.Send` options are configured for origin downloads).

**Tests**: Unit test for `DefaultKrakenTransport()` asserting field values. Integration-level test verifying connection reuse (check `Transport.MaxIdleConnsPerHost` indirectly via metrics if available).

---

## Critical Files

| File | Change |
|------|--------|
| `lib/torrent/scheduler/dispatch/config.go` | PipelineLimit default 3 → 8 |
| `lib/torrent/scheduler/connstate/config.go` | MaxOpenConnectionsPerTorrent default 10 → 20 |
| `agent/agentserver/server.go` | io.Copy → io.CopyBuffer (1 MB) |
| `utils/bandwidth/limiter.go` | Add ReserveIngressCtx / ReserveEgressCtx |
| `lib/torrent/scheduler/conn/conn.go` | Thread ctx into readPayload / sendPiecePayload |
| `utils/httputil/httputil.go` | Add DefaultKrakenTransport() |
| Agent blobclient construction (find exact file) | Wire DefaultKrakenTransport into origin HTTP client |

---

## Verification

1. **Unit tests**: `make unit-test` must pass after each change.
2. **Config defaults**: Grep for hardcoded `3` and `10` in test files for connstate/dispatch to update assertions.
3. **Bandwidth cancellation**: Verify with Go's race detector (`go test -race`) that no goroutine leaks on `Conn.Close()`.
4. **Throughput benchmark**: Run `make devcluster` and measure blob pull time before/after with a large image (1 GB+). Compare `piece_bandwidth` metrics from Prometheus.
5. **No regressions**: Run `make integration` after all changes are complete.
