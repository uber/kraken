# Lazy Pull Streaming Critical Review

## Executive summary

Kraken's lazy pull direction is sound for eStargz. eStargz does not require
Kraken to parse the TOC or understand file-level metadata; stargz-snapshotter
does that through registry byte ranges. Kraken's load-bearing responsibility is
to make those byte ranges resolve to verified torrent pieces quickly, with low
read amplification, and without turning the first cold origin request into a
whole-blob download.

The current branch proves the core idea. The PoC shows eStargz startup moving
from full-pull behavior to range-driven startup, with large reductions in bytes
fetched. The remaining gap is production scheduling and cold-origin behavior:
Stack A makes the agent range-capable, but production eStargz needs cold-origin
range streaming, demand-aware parallelism, better partial-peer discovery, and
observability before it should be treated as fleet-ready.

My main recommendation is to stop treating `in_order` as the production
mechanism for streaming. It was useful for the first PoC because it made low
offset pieces arrive earlier, but the later PoC commits added a better
primitive: lazy demand plus explicit priority hints. Production should keep the
normal global piece policy for non-streaming torrents and add a stream-local
priority scheduler for lazy torrents.

## Scope and evidence

This review covers:

- `docs/lazy-pull-streaming-design.md`
- `docs/lazy-pull-streaming-production-plan.md`
- Current branch implementation on `image-streaming-p2p`
- Dragonfly OSS server checkout at commit
  `0b3cca484543fe22ee1ce1c26550cf9ff6a3e06b`
- Dragonfly OSS client checkout at commit
  `772c024d63eb782ede4875eb52eaa26e46647e6f`

Relevant Kraken PoC commits:

- `5df4d96d` - phase 1 time-to-first-byte PoC. Adds `DownloadReader`,
  `streamReader`, `in_order`, and the first streaming path.
- `59f5ce8e` - phase 2 SOCI. Wires the registry read path to streaming reads,
  adds `ReadAt`/`Seek`/`Size`, and adds priority-piece hints.
- `22d8dcc8` - phase 3 final. Adds lazy demand tracking so the dispatcher only
  requests pieces touched by streaming readers plus readahead.
- `11da1939` - P2P/cold-origin PoC. Adds `RangeDownloader`, `.kmeta`, partial
  origin torrents, and eStargz cold-origin benchmark wiring.

I reviewed each major recommendation three ways before finalizing it:

- Correctness and integrity: does it preserve Kraken's verified-piece model?
- Performance: does it improve time-to-running, first-byte latency, or egress?
- Operability: can it be rolled out, measured, and debugged safely?

Percentages in this document are engineering estimates based on the current
implementation and PoC behavior. They should be validated with the benchmark
plan before production commitments.

## Current Kraken implementation review

### What is strong

The design keeps the right architectural boundary. Kraken remains a
range-capable registry and torrent distribution system. SOCI, eStargz, and
other lazy formats remain snapshotter concerns. This is important because it
keeps Kraken format-agnostic and avoids embedding format-specific TOC parsing
in the agent.

`streamReader` is the right initial abstraction. It lets the registry return a
reader before the blob is complete, while still using the live `storage.Torrent`
that the dispatcher writes into. The reader blocks only on the piece needed by
the current read instead of waiting for the whole blob.

`DownloadReader` reuses an existing torrent control if one exists. That avoids
starting a separate download path for the same blob and lets streaming clients
benefit from any pieces already present.

Phase 3 lazy mode is the key improvement. `Dispatcher.SetLazy` and
`Dispatcher.RequestPieces` make the dispatcher request only demanded pieces.
This is why the PoC can fetch a small fraction of the image instead of quietly
turning streaming into a full pull.

The current code already has some parallelism. `RequestPieces` marks a window of
demanded pieces and kicks `maybeRequestMorePieces` on every peer. The
`piecerequest.Manager` then fills each peer's quota up to `pipeline_limit` or
`origin_pipeline_limit`. This means the current design is not strictly
single-piece-at-a-time.

### What is weak

The current `streamReader` still polls every 5ms for piece availability. That is
simple and acceptable for PoC scale, but it adds avoidable latency to every
blocked piece and burns CPU under many concurrent streams. Piece-arrival
notification should replace polling.

`streamReadahead = 8` is fixed in pieces, not bytes. That means the actual
prefetch budget changes with Kraken's piece size. It is also applied when
`acquirePiece` blocks, including after `ReadAt` has already demanded the exact
span. For random or sparse eStargz reads, fixed readahead can fetch pieces the
snapshotter never asked for.

Demand is monotonic and unclassified. The dispatcher knows a piece is demanded,
but not whether it is the currently blocked piece, an exact range span,
sequential readahead, or optional background fill. Without that distinction, the
scheduler cannot make strong latency decisions.

Priority is global and sorted ascending. `Manager.SetPriority` records pieces in
a single map, and `sortedPriority` returns them in ascending piece order. This
helps one sequential stream, but it can hurt multiple streams or random reads
because lower-index priority pieces can be chosen before the piece a reader is
currently blocked on.

The devcluster config switches `piece_request_policy` to `in_order` globally.
That is a useful benchmark knob, but a risky production default because it also
affects normal non-streaming torrents.

Cold-origin support is the most important production gap. Stack A removes the
agent-side full-blob wait, but without Stack B a cold origin can still
materialize the whole blob before serving pieces. For eStargz at fleet scale,
Stack B is not optional.

## Dragonfly OSS comparison

Dragonfly and Kraken have different architectures, but the streaming lessons map
cleanly.

| Area | Dragonfly behavior | Kraken behavior | Recommendation |
| --- | --- | --- | --- |
| Range-to-piece mapping | Computes all interested pieces for the requested byte range up front. | Dynamically demands pieces from `Read`/`ReadAt`. | Keep dynamic demand, but classify demanded pieces by urgency. |
| Source fallback | Downloads only interested pieces from source with bounded concurrency. | Stack B proposes cold-origin range fetch; Stack A alone may still hit full origin refresh. | Promote Stack B to eStargz production gate. |
| Completion semantics | Ranged peer completion does not imply whole-task completion. | Partial origin reports complete in Stack B because origins do not announce. | Keep this origin-private; never leak into tracker/GC/metrics as real full completion. |
| Parent choice | Client collects parent availability for interested pieces. | Tracker is mostly complete/incomplete; direct handshake bitfields help after connection. | Implement partial-aware discovery, but do not block first-byte latency waiting for all parents. |
| Parallelism | Has explicit concurrent piece count for source/parent downloads. | Has peer/origin pipeline limits, but no streaming-specific concurrency policy. | Add stream-aware reservation classes and backpressure. |
| Read amplification | Whole verified pieces are still the unit. | Same, due to CRC32 piece integrity. | Optimize around the fixed piece grid; do not break it. |

Dragonfly's strongest idea to copy is not its full scheduler architecture. It is
the split between "which pieces are interesting for this request" and "how many
of those pieces may be fetched concurrently from source or parents." Kraken
already has enough infrastructure to express this with lazy demand plus peer
pipeline limits, but it needs a stream-aware priority layer.

Dragonfly's parent collection behavior should not be copied directly. Waiting
for every candidate parent to report availability can improve load balancing,
but eStargz startup is first-byte sensitive. Kraken should dispatch from the
first covering peer and optionally wait only a short bounded window, such as
5-20ms, when multiple peers are likely to cover the same piece.

## `in_order` policy analysis

### What `in_order` does

`in_order` is a piece selection policy that walks the candidate bitset from the
lowest set bit and returns the first valid pieces up to quota. It ignores
rarity, peer coverage quality, current reader offset, and whether a piece is
the blocked piece or speculative readahead.

In the PoC this was configured globally in the devcluster agent config:

```yaml
scheduler:
  dispatch:
    piece_request_policy: in_order
```

That made sense in Phase 1 because the initial streaming reader only read
sequentially. Earlier pieces were usually the pieces the reader would need next.

### When it helps

`in_order` helps when all of these are true:

- There is one dominant sequential reader.
- The requested stream starts near the beginning of the blob.
- The snapshotter's startup path mostly follows increasing offsets.
- The swarm has similar availability for all demanded pieces.
- The goal is first-byte or early sequential progress, not total swarm health.

In this case, `in_order` can reduce time spent fetching far-ahead pieces that do
not unblock the stream. For early PoC validation, that was a pragmatic choice.

### When it hurts

`in_order` can increase latency when the current blocked piece is not the lowest
demanded piece. This can happen with eStargz because the snapshotter may read
TOC/footer bytes, file chunks, and metadata through `ReadAt` rather than a
simple linear scan.

It can also hurt when multiple streaming readers exist for the same torrent. A
reader blocked on a higher piece can lose pipeline slots to lower-index pieces
demanded by another reader or by stale readahead.

It can reduce P2P efficiency because it ignores rarity. If a rare piece is
available from only one partial peer and a common lower-index piece is available
from many peers, global `in_order` may choose the common piece first. That can
lower swarm utilization and increase origin fallback.

It can degrade normal downloads if enabled globally. Non-streaming Kraken
torrents historically use random or rarest-first behavior to spread piece
availability. For full downloads, strict low-index ordering is usually worse for
swarm health.

### Verdict

Do not ship global `in_order` as the production default.

Keep `in_order` only as a debug or benchmark policy. Production should use a
stream-aware priority overlay:

- The blocked piece is always first.
- Exact `ReadAt` span pieces are next.
- Sequential readahead fills remaining capacity.
- Normal rarest-first or default policy handles non-streaming and background
  fill.

Expected impact of replacing global `in_order` with stream-aware priority:

| Impact area | Estimate | Confidence |
| --- | ---: | --- |
| Streaming first-byte latency for random/sparse reads | 5-20% lower | Medium |
| Sequential single-stream TTR | -2% to +5% change | Medium |
| Mixed workload non-streaming regression avoidance | 5-20% | Medium |
| P2P share in partial-peer swarms | 5-15% better | Medium |

## True parallelism while keeping streaming alive

### Design goal

The goal is not to serialize downloads in stream order. The goal is to always
keep the piece that unblocks the reader at the front of the queue while using
all safe peer and origin capacity for adjacent or explicitly requested pieces.

In other words:

- Latency path: blocked piece must be scheduled immediately.
- Throughput path: remaining pipeline slots must stay full.
- Waste control: speculative pieces must be bounded by bytes and disabled for
  random access.
- Integrity path: every served byte still comes from a verified Kraken piece.

### Demand classes

Replace the current unclassified demand bitset with a demand table while still
keeping a bitset for fast filtering.

Suggested model:

```go
type DemandReason int

const (
    DemandBlocked DemandReason = iota
    DemandReadAt
    DemandSequentialReadahead
    DemandBackgroundFill
)

type Demand struct {
    Piece     int
    Reason    DemandReason
    StreamID  uint64
    Deadline  time.Time
    CreatedAt time.Time
}
```

Priority order:

1. `DemandBlocked`: the piece a reader is currently waiting on.
2. `DemandReadAt`: exact pieces covering a registry range read.
3. `DemandSequentialReadahead`: byte-budgeted readahead after the stream
   frontier.
4. `DemandBackgroundFill`: optional tail fill after container-ready.

This preserves streaming latency while still allowing parallel fetching.

### Reservation algorithm

Keep `restrictToDemand` so lazy torrents never request undemanded pieces.
Replace global ascending priority with demand-aware ordering.

For each peer or origin:

1. Compute candidates as today:
   `peer.bitfield INTERSECT localMissing INTERSECT demanded`.
2. Select blocked pieces first, ordered by earliest deadline.
3. Select `ReadAt` pieces next, preferably contiguous spans.
4. Select sequential readahead next, ordered by distance from the stream
   frontier.
5. Fill any remaining streaming background capacity with rarest-first among
   demanded pieces.
6. Do not allow optional background fill to consume slots needed by blocked or
   exact range pieces.

Pseudo-code:

```go
func ReserveStreamingPieces(peerID core.PeerID, candidates *bitset.BitSet, quota int) []int {
    out := selectByClass(candidates, quota, DemandBlocked, earliestDeadline)
    out = append(out, selectByClass(candidates, quota-len(out), DemandReadAt, earliestDeadline)...)
    out = append(out, selectByClass(candidates, quota-len(out), DemandSequentialReadahead, nearestFrontier)...)
    out = append(out, selectRarest(candidates, quota-len(out), DemandBackgroundFill)...)
    return out
}
```

The important property is that the blocked piece does not disable parallelism.
It only takes the first slot. Remaining slots still fetch useful pieces.

### Stream reader changes

Change sequential reads to maintain a moving frontier.

Current behavior demands readahead only when `acquirePiece` blocks. That can
leave a gap at piece boundaries if the previous window was consumed before the
next window was requested.

Recommended behavior:

- On opening or advancing a sequential piece, demand the next byte-budgeted
  window immediately.
- Mark the current piece as `DemandBlocked` only when the reader actually waits.
- Mark future pieces as `DemandSequentialReadahead`.
- When a sequential reader seeks, start a new stream frontier and retire stale
  readahead if no other reader still wants it.
- For `ReadAt`, demand only the exact span as `DemandReadAt`; no speculative
  neighbors.

Expected improvement:

- 0-8% lower sequential time-to-running from fewer dry pipeline moments.
- 10-35% lower wasted bytes for sparse/random reads when combined with
  byte-budgeted readahead.
- Medium-high confidence for waste reduction, medium confidence for TTR.

### Piece-arrival notification

Replace `streamPollInterval = 5ms` polling with piece completion notification.

Possible implementation:

- Add a lightweight notifier around the dispatcher/torrent write path.
- On successful `WritePiece`, broadcast to waiters for that piece.
- `streamReader.acquirePiece` waits on the piece-specific channel or condition.
- Terminal torrent errors still wake all waiters.

Expected improvement:

- 1-5% lower time-to-running for mostly sequential eStargz.
- 5-15% lower latency for many small range reads or high RTT.
- 2-10% lower CPU under many concurrent streams.
- Medium confidence.

### Cold-origin concurrency

The partial origin code in `11da1939` has the right integrity shape:

- `.kmeta` gives cold origin the real `MetaInfo`.
- `RangeDownloader` fetches the exact piece bytes.
- CRC32 is verified against the metainfo piece sum.
- Concurrent requests for the same piece are coordinated with dirty/complete
  piece state.

Production needs stronger concurrency controls:

- Per-origin max concurrent range fetches.
- Per-blob max concurrent range fetches.
- Duplicate request coalescing with waiter notification instead of polling.
- Backoff and retry policy for failed range fetches.
- Metrics for range 206 count, bytes fetched, duplicate waiters, and fallback
  to full refresh.

Expected improvement:

- 10-30% lower cold-origin eStargz time-to-running.
- 20-50% lower p99 backend latency under concurrent cold starts.
- 15-35% lower origin/backend bytes when combined with partial-aware peer
  discovery.
- Medium confidence; workload dependent.

### Partial-peer discovery

Kraken already exchanges bitfields directly after peers connect, but the tracker
does not know which incomplete peers cover the pieces a streaming leecher needs.
That causes avoidable origin fallback during cold starts.

Recommended tracker behavior:

- Add V3 announce support with packed bitfield or compact ranges.
- Include a bounded `requested_pieces` field only for streaming/lazy peers.
- Rank handout by coverage of requested pieces, locality, load, and completeness.
- Keep origins as fallback, not first choice when a covering peer exists.

Do not serialize huge `[]bool` arrays in production. Use packed bytes or ranges
and cap request context size.

Expected improvement:

- P2P share from current measured 35-44% toward 50-70% in warm-peer starts.
- 15-35% lower backend/origin bytes.
- 5-20% lower time-to-running for multi-agent starts.
- Medium confidence.

## eStargz-specific production recommendations

### Keep Kraken format-agnostic

Do not add eStargz TOC parsing to Kraken core for v1. The snapshotter should
continue to own TOC interpretation. Kraken should only guarantee:

- Correct HTTP range behavior.
- Stable `Content-Length`.
- No silent full-pull fallback.
- Verified piece serving.
- Efficient range-to-piece demand scheduling.

### Make Stack B part of the v1 production gate

For eStargz, Stack A alone is a warm-cache optimization. It does not fully solve
cold fleet behavior if origins still need a full backend download before serving
pieces. Stack B should be treated as required for production eStargz streaming.

Stack B must resolve:

- `.kmeta` sidecar write, read, delete, and GC lifecycle.
- Pather compatibility for production backends.
- Range support for GCS and S3, not only testfs.
- Metrics that distinguish partial-origin logical completeness from real
  whole-blob cache completeness.
- Fallback behavior when sidecar or range support is absent.

### Tune eStargz chunk size against Kraken pieces

The PoC eStargz run fetched fewer bytes than SOCI on `pytorch`, but required
more ranged GETs and was slower. The likely reason is finer chunking from
`--estargz-min-chunk-size=0`, which improves byte granularity but increases
round trips and piece touches.

Benchmark:

- `--estargz-min-chunk-size=0`
- `64KiB`
- `128KiB`
- `256KiB`
- `1MiB`

Measure:

- Time-to-running.
- First-byte latency.
- Number of HTTP 206 responses.
- Distinct Kraken pieces fetched.
- Bytes fetched from agent, peers, origin, and backend.
- Snapshotter fallback count.

Expected improvement:

- 5-20% lower time-to-running.
- 20-60% fewer ranged GETs.
- Bytes may increase 0-15%.
- Medium-high confidence.

## Performance estimates

| Improvement | Expected gain | Confidence | Notes |
| --- | ---: | --- | --- |
| Stack B cold-origin range streaming | 30-80% lower cold-origin TTR; 80-96% fewer backend bytes on sparse startup | High | Required for production eStargz. |
| Stream-aware priority instead of global `in_order` | 5-20% lower random/sparse read latency; avoids 5-20% mixed workload regression | Medium | Keeps blocked piece first without serializing whole torrent. |
| Byte-budgeted sequential-only readahead | 10-35% less wasted traffic; 0-8% TTR gain | High | Especially important for `ReadAt`. |
| Piece-arrival notification | 1-5% sequential TTR; 5-15% small-read latency; 2-10% CPU reduction | Medium | Replaces 5ms polling. |
| Origin/source concurrency and coalescing | 10-30% cold TTR; 20-50% lower p99 backend tail under concurrent starts | Medium | Needs careful backpressure. |
| Partial-aware tracker handout | P2P share from 35-44% toward 50-70%; 5-20% TTR gain | Medium | Depends on concurrent starts and image reuse. |
| eStargz chunk tuning | 5-20% TTR; 20-60% fewer ranged GETs | Medium-high | Must balance bytes and round trips. |
| Optional async tail fill | 10-40% faster second-start/subsequent reads | Medium-low | Can increase bytes; must be background and rate limited. |

## Implementation plan

### Phase 1 - Document and guard the current PoC

- Keep this review next to the existing design docs.
- Keep global `in_order` only in devcluster or benchmark configs.
- Document that production default remains `rarest_first` unless a torrent is in
  lazy streaming mode.
- Add metrics for demanded pieces, requested pieces, wasted readahead, and
  local/P2P/origin/backend split.

### Phase 2 - Replace global `in_order` with stream-aware priority

- Add demand classes to the dispatcher.
- Replace `SetPriorityPiece(piece int)` with a richer demand update API.
- Keep a fast demand bitset for filtering candidates.
- Update `Manager.ReservePieces` to reserve by streaming priority classes before
  falling back to the existing policy.
- Scope this behavior to lazy streaming torrents only.

### Phase 3 - Improve stream reader demand

- Replace fixed `streamReadahead` with `streamReadaheadBytes`.
- Compute readahead piece count from the torrent piece length.
- Demand sequential windows proactively.
- Disable speculative readahead for `ReadAt`.
- Add stream IDs so stale readahead from one stream can be distinguished from
  active demand from another stream.

### Phase 4 - Replace polling with notification

- Add piece completion notification after successful `WritePiece`.
- Add terminal wakeup for torrent error, timeout, and removal.
- Update `streamReader.acquirePiece` to wait on notification.
- Keep a short timeout only as a safety fallback, not the normal mechanism.

### Phase 5 - Harden cold-origin range streaming

- Implement `RangeDownloader` for production GCS and S3 backends.
- Add per-origin and per-blob concurrency limits.
- Replace partial-origin dirty polling with waiter notification.
- Ensure `.kmeta` sidecar deletion follows blob deletion or a TTL sweep.
- Add fallback metrics for sidecar miss, range unsupported, and full refresh.

### Phase 6 - Add partial-aware tracker handout

- Add V3 announce with compact bitfield/progress.
- Add bounded requested-piece context for lazy peers.
- Rank peers by requested-piece coverage, locality, load, and completeness.
- Keep V1/V2 compatibility and make the policy opt-in.

## Test plan

### Unit tests

- `Manager` reserves blocked pieces before readahead.
- `Manager` still fills remaining quota after reserving blocked pieces.
- Multiple stream IDs do not starve a higher-offset blocked piece behind
  lower-index stale readahead.
- `ReadAt` demands exact span only and does not demand adjacent pieces.
- Sequential reads demand byte-budgeted windows.
- Piece notification wakes all waiters for the completed piece.
- Terminal torrent errors wake blocked readers.
- Cold-origin duplicate requests for the same piece trigger one backend
  `DownloadRange`.

### Integration tests

- Existing scheduler and dispatch tests pass with streaming priority disabled.
- Lazy dispatcher requests no pieces before demand.
- Lazy dispatcher requests only demanded pieces.
- Registry range requests return correct `206` content for suffix, open-ended,
  bounded, and full-object ranges.
- Cold origin with `.kmeta` performs range 206 fetches and zero full-blob 200
  fetches for lazy startup.

### Benchmarks

- `python:3.12` eStargz cold-agent, warm-origin.
- `python:3.12` eStargz cold-agent, cold-origin.
- `pytorch 2.5.1-cuda12.4-cudnn9-devel` eStargz cold-agent, warm-origin.
- Same `pytorch` run with 2, 5, and 20 concurrent agents.
- eStargz chunk-size sweep.
- Synthetic sparse `ReadAt` workload.
- Mixed workload with one streaming torrent and normal full torrents to verify
  no global `in_order` regression.

## Final recommendation

Ship eStargz streaming only after the plan treats Stack B and stream-aware
parallelism as production requirements. The current branch proves the concept,
but global `in_order`, fixed piece readahead, polling, and Stack A-only origin
behavior are PoC choices.

The production shape should be:

- Kraken stays format-agnostic.
- `rarest_first` remains the normal global policy.
- Lazy torrents get a stream-aware priority overlay.
- Blocked pieces are first, but peer and origin pipelines remain full.
- Cold origins range-fetch verified pieces, not whole blobs.
- Partial-peer discovery improves P2P share.
- Every fallback and read-amplification path is observable.
