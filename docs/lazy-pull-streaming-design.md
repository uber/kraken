# Lazy-Pull / Image Streaming for Kraken — Design One-Pager

Status: Draft · Owner: TBD · Date: 2026-06-10

## Goal

Let a container runtime start before a layer is fully present, by serving
**arbitrary byte ranges of a blob on demand**, with the missing pieces fetched
P2P and prioritized by what the runtime is actually reading. The image format
that describes *which* ranges to fetch (SOCI ZToC, eStargz TOC, Nydus, custom)
must be **pluggable** — Kraken should not hardwire AWS Labs' SOCI.

## Non-goals (v1)

- Re-packing images into a new on-disk format. We keep standard OCI blobs.
- A FUSE filesystem inside Kraken. The runtime-side snapshotter
  (soci-snapshotter / stargz-snapshotter / nydus) stays the FUSE provider; the
  Kraken **agent is its registry**, made range-capable.
- Changes to containerd core or to dockerd (see "Runtime integration").

## The one constraint everything traces back to

Kraken is **whole-blob, all-or-nothing**. The registry read path blocks until
the entire blob is torrent-downloaded and committed to cache:

- `lib/dockerregistry/transfer/ro_transferer.go:72` `Download` → `sched.Download`
  blocks on `<-errc` (`lib/torrent/scheduler/scheduler.go:256`), which is only
  signaled on full completion (`lib/torrent/scheduler/events.go:359`
  `dispatcherCompleteEvent`).
- "Complete" ≡ download file moved to cache:
  `lib/torrent/storage/agentstorage/torrent.go:121` `Complete()`,
  `:238` `MoveDownloadFileToCache`.
- The agent then serves the whole file with `io.Copy`, **no Range support**:
  `agent/agentserver/server.go:167`.

But the substrate underneath is already piece-based and the gap is narrow:

- Pieces are **independently verified** (CRC32 per piece) the instant they land:
  `lib/torrent/storage/agentstorage/torrent.go:192`.
- The per-piece reader already reads from the **in-progress download file**, not
  just cache: `opener.Open` uses `cads.Any()`
  (`lib/torrent/storage/agentstorage/torrent.go:256`).
- `Torrent` already exposes `HasPiece`, `Bitfield`, `PieceLength`,
  `GetPieceReader`, `NumPieces`, `Length` (`lib/torrent/storage/storage.go`).

So the work is: (1) a streaming reader that waits per-piece instead of
per-blob, (2) piece prioritization by byte range, (3) Range on the agent HTTP
path, (4) a pluggable index artifact distributed via the existing tag system,
and (optionally) (5) range fetch from the backend on origin + (6) partial
peer discovery on the tracker.

---

## Architecture

```
 push: docker push ─▶ proxy (rw_transferer) ─▶ origins (blobs by digest)
                                            └▶ build-index (tag→manifest)
       indexer  ─────▶ proxy ─▶ origins (index blob by digest)
                              └▶ build-index (derived tag → index digest)   [pluggable resolver]

 pull: runtime snapshotter ─range GET▶ agent registry ─▶ streaming reader
                                                          │ piece [a,b) prioritized
                                                          ▼
                                          scheduler (P2P) ◀─▶ peers / origins
```

### Pluggable format abstraction (new)

New package `lib/streaming` defines the seam so no Kraken core code knows what
SOCI/eStargz/Nydus is:

```go
// lib/streaming/format.go
package streaming

// IndexFormat is a registered streaming-index handler (soci, estargz, nydus, ...).
type IndexFormat interface {
    // Name is the format key, e.g. "soci". Used in the derived tag suffix.
    Name() string
    // DependencyDigests parses an index blob and returns the data blobs it
    // references, so build-index can verify + replicate them.
    DependencyDigests(index io.Reader) (core.DigestList, error)
}

// Registry maps format name -> IndexFormat. Populated via Register() at init.
```

`soci`, `estargz`, `nydus` are separate sub-packages implementing this; only the
ones compiled in are active. v1 ships `soci` (no image conversion needed) and a
trivial `passthrough`. This is the single place that grows when adding a format.

### Index discovery: derived tag, not OCI referrers

Kraken has **no referrers/subject/artifactType** concept (grep confirms zero
hits) but does have a generic `tag→digest` KV with HA + cross-cluster
replication. So the index is just a normal blob, discovered by a derived tag:

```
<repo>:sha256-<manifest-digest>.<format>      e.g.  myrepo:sha256-abc123.soci
```

- Push: push index blob via existing `Upload` (origins store it by digest — no
  change), then `PutTag(derivedTag, indexDigest)` via existing
  `tagclient.PutAndReplicate` (`build-index/tagclient/client.go:90`).
- Pull: agent computes the derived tag from the manifest digest and resolves it
  with the existing `GetTag` (`ro_transferer.go:96`). Deterministic, no new API.

---

## Exact code changes, by layer

### 1. Store — expose in-progress reads (small)

`lib/store/ca_download_store.go` already supports reading download-state files
via `Any()`/`Download()` scopes (`:155`). No new store primitive strictly
required; the streaming reader uses `GetPieceReader` which already opens via
`Any()`. **No change** beyond confirming `Download().GetFileStat` is callable
for size before completion (it is). Keep this layer untouched to limit blast
radius.

### 2. Torrent / scheduler — streaming reader + per-piece signal + priority (core)

a. **Per-piece completion signal.** In
`lib/torrent/scheduler/dispatch/dispatcher.go`, after `WritePiece` succeeds in
`handlePiecePayload` (`:589`–`:604`), broadcast the completed piece index to a
new per-dispatcher subscriber set (a `sync.Cond` or `chan struct{}` fan-out
keyed by piece). Today the only signal is the all-pieces `DispatcherComplete`.

b. **Non-blocking download + reader on the `Scheduler` interface.**
`lib/torrent/scheduler/scheduler.go:51`. Add:

```go
// DownloadReader registers/starts leeching the torrent and returns a reader
// that blocks only on the pieces covering each Read, not the whole blob.
DownloadReader(namespace string, d core.Digest) (store.FileReader, error)
```

Internally it sends the existing `newTorrentEvent` (to start leeching) but does
**not** wait on `errc`. Returns a `*streamReader`.

c. **`streamReader`** (new, `lib/torrent/scheduler/stream_reader.go`),
implementing `store.FileReader` (`Read`/`ReadAt`/`Seek`/`Close`/`Size`):
- maps offset→piece with `piece = off / mi.PieceLength()` (offset math already
  exists as `getFileOffset`);
- on `Read`/`ReadAt`, sets the dispatcher's **priority range** = pieces covering
  `[off, off+len)`, then for each needed piece: if `HasPiece` read via
  `GetPieceReader`; else wait on the per-piece signal (b/a) until it lands;
- `Size()` from `Torrent.Length()` (known from metainfo up front).

d. **Piece prioritization by range.** Add a `priorityPieces *bitset.BitSet` to
`Dispatcher`, settable from `streamReader`. In `maybeSendPieceRequests`
(`dispatcher.go:413`) and `resendFailedPieceRequests` (`:434`), reserve pieces
from `pieceCandidates ∩ priorityPieces` **first**, then fill remaining pipeline
quota with the existing policy. This keeps the
`piecerequest.pieceSelectionPolicy` interface
(`lib/torrent/scheduler/dispatch/piecerequest/policy.go:25`) unchanged — random
/ rarest-first still handle the non-priority tail.

e. **Preserve the old path.** Existing `Download` (blocking) stays for proxy
preload + replication. `dispatcherCompleteEvent`→`errc` (`events.go:359`)
untouched.

### 3. Agent registry — Range support + format wiring (small)

a. `lib/dockerregistry/transfer/ro_transferer.go`: switch `Download` (`:72`) to
`sched.DownloadReader`, and `Stat` (`:55`) to use metainfo size without forcing
full download (use `sched`'s torrent stat). The driver already calls
`Reader(ctx, path, offset)` and `Seek` (`blobs.go:82`,`:113`).

b. `agent/agentserver/server.go` `downloadBlobHandler` (`:146`): replace
`io.Copy` (`:167`) with `http.ServeContent(w, r, name, modtime, readSeeker)` so
HTTP `Range:` requests are honored against the `streamReader`. This is the byte
the snapshotter actually calls.

### 4. Backend — range download (medium, needed for origin streaming)

`lib/backend/client.go:50` `Client` has only whole-object `Download`. Add an
**optional** capability interface (don't break existing backends):

```go
// lib/backend/client.go
type RangeDownloader interface {
    DownloadRange(namespace, name string, offset, length int64, dst io.Writer) error
}
```

- S3: `s3backend/client.go:194` set `input.Range = "bytes=a-b"` (s3manager
  already does ranged multipart internally).
- GCS: `gcsbackend` `obj.NewRangeReader(ctx, off, len)`.
- HDFS/http/registry: set HTTP `Range` header.
Backends not implementing `RangeDownloader` fall back to whole-blob (current
behavior) — origin streaming is simply disabled for them.

### 5. Origin — range-fetch + partial seed (medium/large, phase 2)

Today origin is a 100%-complete seeder: `blobrefresh/refresher.go:139`
`download` pulls the whole object via one `client.Download` into one CAStore
file; `originstorage/torrent.go` hardcodes `Complete()=true`, `HasPiece=true`,
`WritePiece=ErrReadOnly`.

Two options:
- **Phase 1 (cheap, ship first):** leave origin whole-blob. On the *first* agent
  range request the origin still materializes the full blob async (existing 202
  path, `origin/blobserver/server.go:577`). Streaming benefit is **agent↔peer**
  P2P + warm-cache origins. Correct and simple; cold origin = whole fetch.
- **Phase 2 (true cold streaming):** give origin a partial store modeled on
  `agentstorage` (sparse file + per-piece bitfield), add a `download_range`
  refresher path using `RangeDownloader` keyed per (digest, piece) in the dedup
  cache, and let `originstorage.Torrent` reflect a real bitfield so origin can
  **seed partial content**. Largest change; defer until Phase 1 proves value.

### 6. Proxy + build-index — distribute & discover the index (small)

- Proxy push path is unchanged: the index blob rides the existing `Upload`
  (`rw_transferer.go:193`) and the derived tag rides `PutTag`/`PutAndReplicate`
  (`rw_transferer.go:234`).
- **New dependency resolver** in `build-index/tagtype/`: register a resolver
  (alongside `"docker"`/`"default"` in `map.go:70`) for the
  `*.soci`/`*.<format>` namespace that calls
  `streaming.Registry[format].DependencyDigests(...)`. This makes build-index
  (a) verify the index's data blobs exist on origin before accepting the tag
  (`tagserver/server.go:513`) and (b) replicate them cross-cluster
  (`:569`, `tagreplication.NewTask(tag, d, deps, ...)`). Without this the index
  tag replicates but its referenced blobs might not.

### 7. Tracker — partial-aware discovery (optional, phase 2)

The tracker is piece-agnostic: it stores a binary have + a single `Complete`
bool per (blob, peer) (`core/peer_info.go:19`,
`tracker/peerstore/redis.go:35`). Streaming works without changing it (agents
exchange bitfields directly in the dispatch handshake,
`conn/handshaker.go:34`). For better cold-start, **phase 2**: add a V3 announce
carrying progress/bitfield (extend `PeerInfo` + `announceclient.Request`,
`announceclient/client.go:36`), store it in the peerstore, and add a handout
policy that prefers peers covering the requested pieces. Defer.

---

## zstd interaction (do this right or it breaks streaming)

There is **no zstd code or design in the repo today** (grep: zero `zstd`,
`klauspost`, `seekable`); the in-flight `docs/*cache*` work is off-heap caching,
not compression. The hard rule for compatibility with streaming:

- Digest and piece CRCs are over the **stored bytes**
  (`core/digester.go`, `core/metainfo.go` `calcPieceSums`), and piece reads are
  linear seeks `offset = piece * pieceLength`
  (`storage/piecereader/file.go:96`). **Whole-blob single-stream zstd destroys
  this 1:1 offset mapping and is incompatible with byte-range / lazy pull.**
- The two compatible designs:
  1. **Per-piece zstd** — each `PieceLength` chunk is an independent zstd frame.
     Frame boundary == piece boundary, so offset math is preserved; compress for
     transport/at-rest, decompress + verify on read. Cleanest fit; recommend
     coordinating the zstd effort to land **per-piece**, not whole-blob.
  2. **zstd seekable format** with an explicit uncompressed→compressed offset
     index (skippable frames). More machinery; only if cross-tool seekable-zstd
     compat is required.
- OCI digests are over **uncompressed** layer bytes the client expects, so
  at-rest compression also needs the digest kept over uncompressed bytes (a
  CAStore change). Keep at-rest compression out of v1; if pursued, gate it
  behind the per-piece model above.

Net: streaming and zstd are compatible **iff zstd is per-piece**. Flag this to
the zstd workstream now — it's a cheap constraint to honor early and expensive
to retrofit.

---

## Phasing

1. **P1 — agent-side streaming (highest value/effort ratio):** §2 (reader +
   per-piece signal + range priority), §3 (Range on agent), §6 (pluggable index
   + resolver), integrate soci-snapshotter against the agent. Origin stays
   whole-blob (§5 Phase 1). No tracker/backend change.
2. **P2 — cold-origin streaming:** §4 (backend range) + §5 Phase 2 (partial
   origin seed) + §7 (partial tracker discovery).
3. **P3 — compression:** per-piece zstd, coordinated with the zstd workstream.

---

## Test & benchmark plan (before/after)

Goal metrics: **time-to-first-byte (TTFB)** and **time-to-container-ready**, vs
baseline whole-blob pull, across blob-size buckets.

### Harness (reuse what exists)

- **End-to-end + TTFB:** extend `tools/bin/puller/pull.go` (already times pulls:
  logs `time.Since(t)`); add a first-byte timer in `pullLayer` and per-layer +
  total output. Builds natively (`make tools`), already a dep of `integration`.
- **Integration:** `test/python/test_docker.py` + `components.py:65 pull()`
  (Python pytest, `make integration`). Add a streaming test that pulls a large
  image and asserts TTFB ≪ full-pull time. Add a fixture toggling streaming
  on/off to A/B the same image.
- **Devcluster** for manual runs (`make devcluster`; ports proxy 15000, origin
  15002, tracker 15003, build-index 15004, agents 16002/17002).

### In-process metrics (already emitted)

`lib/observability/download_performance.go` emits latency + throughput
histograms bucketed by blob size:
- `TORRENT_DOWNLOAD` (`download_time`, agent e2e),
- `TORRENT_LEECH` (`p2p_leech_throughput`),
- `METAINFO_DOWNLOAD`, `REMOTE_DOWNLOAD`, `PROXY_BLOB_DOWNLOAD`.

Add a new `DownloadType` `STREAM_TTFB` emitting time from request to first piece
served, and `stream_pieces_served` / `stream_pieces_waited` counters in
`streamReader`. Assert on these via `tally.NewTestScope` in unit tests.

### Microbenchmarks (established pattern)

Follow `origin/blobclient/transfer_bench_test.go` (`BenchmarkTransferBlob`,
custom `b.ReportMetric`) and the `bench-results/run*.sh` + `benchstat`
before/after methodology already in the repo:
- `BenchmarkStreamReader_FirstPieceLatency` — synthetic torrent, measure time
  from `Read` to first byte vs. `Download` full-blob, swept over piece count.
- `BenchmarkPiecePriority` — verify priority pieces are reserved before the
  random/rarest tail (assert request order, not just timing).

### A/B procedure

1. Pick representative images (one small ~50MB, one large ~2GB many-layer).
2. Baseline: current `master`, `puller` cold pull ×N, record TTFB + total via
   `benchstat`.
3. Streaming: feature branch + index pushed, soci-snapshotter cold pull ×N.
4. Compare TTFB, total, bytes-fetched-before-start, and P2P leech throughput
   from the observability histograms. Expect large TTFB win, neutral-to-slightly
   higher total bytes (range overhead), no regression on the non-streaming path.

---

## Runtime integration (no containerd/dockerd core changes)

- **containerd:** reuse the existing remote-snapshotter API. Run
  soci/stargz/nydus snapshotter pointed at the **agent** as its registry
  (`localhost`). Pure config (register snapshotter, CRI `snapshotter` field).
- **dockerd:** out of scope — lazy pull requires containerd's snapshotter API;
  classic dockerd graphdriver hooking is not worth it. Target containerd/k8s.

## Open questions

1. Who produces the index at push time — the proxy (inline, on manifest put) or
   an external `kraken-indexer` job? Inline couples push latency to indexing;
   external needs an orchestration hook. Lean external for v1.
2. Index immutability vs. tag mutation: derived tag is content-addressed by
   manifest digest, so it's immutable — good. Confirm GC keeps the index blob
   pinned as long as the image is referenced.
3. Piece length vs. snapshotter chunk size: SOCI/eStargz chunks won't align with
   Kraken `PieceLength` (size-bucketed, `lib/metainfogen/config.go:70`). Range
   priority must round to piece boundaries; quantify read amplification.
