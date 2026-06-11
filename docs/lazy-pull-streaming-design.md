# Lazy-Pull / Image Streaming for Kraken — Design One-Pager

Status: PoC complete (Phase 1 + 3) · Owner: TBD · Date: 2026-06-11

A working end-to-end PoC has shipped on branch `image-streaming` and is
validated in the devcluster against soci-snapshotter — see
[PoC results](#poc-results-2026-06-11). The sections below mark what is
**as-built** vs. still **design**.

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

## PoC results (2026-06-11)

A Phase 1 + Phase 3 PoC is implemented on branch `image-streaming` and measured
end-to-end in the devcluster, pulling real images through the Kraken agent
registry with **soci-snapshotter** doing the lazy mount. The metric is
**time-to-running**: a single `nerdctl run` (cold cache) that auto-pulls and
starts the container, overlayfs (full pull) vs. soci (lazy range pull). Each leg
targets a separate cold agent so caches never cross-warm.

| image | overlayfs | soci (lazy) | speedup | bytes overlayfs | bytes soci | lazy layers |
|-------|-----------|-------------|---------|-----------------|------------|-------------|
| pytorch 2.5.1-cuda12.4-cudnn9-devel (~6.9 GiB) | 144.75 s | **13.48 s** | **~10.7×** | 7071.6 MiB (16× full `200`) | **325.1 MiB** (239× ranged `206`) | 5 |
| anaconda3 (~3 GiB) | 58.04 s | **6.94 s** | **~8.4×** | — | — | — |

For pytorch the container reaches running after fetching **~4.6%** of the image
(21.7× fewer bytes from Kraken). The byte savings come from **demand-driven
piece fetch** (Phase 3): the agent only downloads the pieces soci actually reads,
not the whole blob. Lazy-ness was verified per run — 5 layers logged
`remote-snapshot-prepared:true` and there were **0** HTTPS-fallback errors (a
non-zero count would mean the artifact fetch silently fell back to a full pull).

**Harness:** `examples/devcluster/soci/` over a running `make devcluster`. The
host driver `soci_benchmark.sh` builds + starts a privileged DinD container
(containerd + soci-snapshotter + `nerdctl`/`soci`/`ctr`) that runs `run_e2e.sh`
as its workload; results are read back from `docker logs` (deliberately not
`docker exec`, which needs a connection upgrade the busy daemon transiently
refuses). The 206-vs-200 byte split is computed from the agent nginx access logs.

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

> **Status:** design only. The PoC did **not** build `lib/streaming` (grep:
> zero `IndexFormat`/`DependencyDigests` hits). The soci index was pushed as an
> ordinary blob via `soci push --existing-index allow` and discovered through the
> fallback tag below — single cluster, so no dependency resolver was needed yet.

### Format support: soci vs estargz vs nydus

The byte-level read path is **format-agnostic**: any snapshotter that issues
ranged GETs on layer blobs is served by the same streaming reader. The formats
differ only in *where the chunk index lives* and *whether the image must be
converted* — which determines how much (if anything) Kraken must add.

| format | chunk index | image conversion | discovery | what Kraken must add |
|--------|-------------|------------------|-----------|----------------------|
| **soci** | separate index artifact (ztoc blobs + index) | **none** (works on stock OCI images) | referrers → fallback `sha256-<digest>` tag | cross-cluster: a resolver to replicate the index's data blobs (§6). Nothing for single-cluster. |
| **estargz** | embedded as a TOC footer **inside each layer blob** | **required** (`nerdctl image convert --estargz` / `ctr-remote`) | implicit — no separate artifact, no referrers | **nothing in core** — converted layers are opaque blobs; range reads suffice. |
| **nydus** | separate bootstrap + blob artifacts | required (RAFS) | manifest references | soci-style index distribution (later). |

Key consequence: **estargz is already supported by the PoC with no Kraken
change** — Kraken stores/serves blobs opaquely by digest (`rw_transferer.Upload`
ignores media type), so estargz-converted layers push through unchanged and the
same range path serves them. The only cost is push-time conversion, a client/CI
concern. soci's advantage is needing **no conversion**; that is why the PoC used
it. (`--estargz-external-toc` moves the TOC into a separate "TOC image" pushed to
the same registry — still just blobs + tags, still no referrers.)

### Index discovery: derived tag, not OCI referrers — and do we need Referrers?

Kraken has **no referrers/subject/artifactType** concept (grep confirms zero
non-vendor hits) but does have a generic `tag→digest` KV with HA + cross-cluster
replication. So the index is just a normal blob, discovered by a derived tag:

```
<repo>:sha256-<manifest-digest>.<format>      e.g.  myrepo:sha256-abc123.soci
```

- Push: push index blob via existing `Upload` (origins store it by digest — no
  change), then `PutTag(derivedTag, indexDigest)` via existing
  `tagclient.PutAndReplicate` (`build-index/tagclient/client.go:90`).
- Pull: agent computes the derived tag from the manifest digest and resolves it
  with the existing `GetTag` (`ro_transferer.go:96`). Deterministic, no new API.

**Do we need to implement the OCI Referrers API? No — not a blocker.** soci and
stargz both auto-fall back to the OCI `sha256-<manifest-digest>` tag scheme when
a registry lacks the Referrers API, and Kraken's tag system already supports it
(tag GET plus prefix listing: `build-index/tagserver/server.go:131`
`/repositories/{repo}/tags`, `tagclient/client.go` `ListRepository`/`List`). The
PoC ran entirely on this fallback (5 lazy layers, ranged reads, 0 errors). Kraken
also *can't* get Referrers for free: the vendored `github.com/docker/distribution`
is **v2.7.1** (2019 pin in `go.mod`), which predates the Referrers API
(distribution v3 / registry 2.8+) — its route table has no `referrers` route.

Implementing Referrers is an **optional future enhancement** (fewer round trips,
no client-side filtering; some newer tooling such as SOCI Index Manifest v2 / ECS
prefers it). If pursued: serve `GET /v2/<name>/referrers/<digest>` at the
proxy/agent registry, backed by a new build-index **digest → referring-artifacts**
index. That mapping is the missing state today; everything else (blob storage,
tags) already exists.

---

## Exact code changes (as-built)

> What actually shipped on branch `image-streaming` for the PoC, layer by layer.
> Layers 1–3 are **built and validated**; layers 4–7 remain **design only** (the
> PoC did not need cold-origin streaming — see Phasing). Two deviations from the
> original design are called out inline: the reader **polls** instead of waiting
> on a per-piece signal, and HTTP `Range` is served by the registry read path's
> vendored `http.ServeContent`, not by a new agent endpoint.

### 1. Store — expose in-progress reads (no change, as predicted)

`lib/store/ca_download_store.go` already supports reading download-state files
via `Any()`/`Download()` scopes (`:155`). No new store primitive strictly
required; the streaming reader uses `GetPieceReader` which already opens via
`Any()`. **No change** beyond confirming `Download().GetFileStat` is callable
for size before completion (it is). Keep this layer untouched to limit blast
radius.

### 2. Torrent / scheduler — streaming reader + demand-driven fetch (core, built)

This is the centerpiece of the PoC. The win comes from **demand-driven piece
fetch**: a lazily-opened torrent only requests the pieces a reader actually
touches (plus readahead), instead of the whole blob.

a. **Streaming entry point on the `Scheduler` interface.**
`lib/torrent/scheduler/scheduler.go` adds a `BlobReader` interface
(`io.ReadSeekCloser + io.ReaderAt + Size()`) and
`DownloadReader(namespace, d) (BlobReader, error)`. It sends a new
`streamTorrentEvent` (`events.go`) that **reuses the live torrent control** if
one exists, calls `ctrl.dispatcher.SetLazy()`, and returns a `*streamReader`
over `ctrl.torrent` (`state.go` now holds the live `storage.Torrent` instance so
the reader's `HasPiece` reflects pieces as they land). The blocking `Download`
path is untouched for proxy preload + replication.

b. **`streamReader`** (`lib/torrent/scheduler/stream_reader.go`, new) implements
`BlobReader`. **Deviation from original §2a:** rather than a per-piece
`sync.Cond`/channel fan-out broadcast from `WritePiece`, the reader **polls** the
live torrent (`streamPollInterval = 5ms`) for `HasPiece`, with a terminal `errc`
for fatal torrent errors. Polling was simpler and adequate at PoC scale; the
fan-out remains a future optimization if 5ms latency per uncached piece matters.
On each `Read`/`ReadAt` it computes the covering piece span, calls `demand()` to
register those pieces (+ `streamReadahead = 8` pieces ahead) with the dispatcher,
waits for them via `acquirePiece`, then reads through `GetPieceReader`. `Size()`
comes from the metainfo length up front.

c. **Demand set + lazy mode on the dispatcher.**
`lib/torrent/scheduler/dispatch/dispatcher.go` adds `demandMu sync.Mutex`,
`lazy bool`, and a `demand *bitset.BitSet`. `SetLazy()` flips the torrent into
lazy mode; `RequestPieces()` ORs newly-demanded pieces into the set;
`restrictToDemand()` intersects piece candidates with `demand` so that, in lazy
mode, **only demanded pieces are ever requested**. The intersection is applied in
both `maybeRequestMorePieces` and `resendFailedPieceRequests`. A
`lazy_pieces_requested` counter and a teardown log line ("demanded N/total")
make the savings observable — this is what produced the **325 MiB vs 7072 MiB**
result.

d. **In-order priority piece selection.**
`lib/torrent/scheduler/dispatch/piecerequest/in_order_policy.go` (new) adds
`InOrderPolicy = "in_order"`, selecting the lowest-index candidate pieces via
`candidates.NextSet` so a streaming reader gets bytes roughly front-to-back.
`manager.go` gains a `priority map[int]struct{}` with `SetPriority`/`Clear` and
reserves priority pieces first in `ReservePieces`. The existing random/
rarest-first policies are unchanged and still handle non-streaming torrents.

### 3. Agent registry — Range support, as-built (built)

**Deviation from original §3.** The original plan was to add
`http.ServeContent` to the agent's `downloadBlobHandler`. In practice the
snapshotter pulls through the **Docker registry read path**, not the raw blob
endpoint, and that path already serves HTTP `Range`:

a. `lib/dockerregistry/transfer/ro_transferer.go` — `Stat` returns
`core.NewBlobInfo(r.Size())` from the metainfo via `DownloadReader` (no full
download); `Download` returns the streaming reader on a cache miss and increments
`mb_served`. This is the path soci actually used in the PoC.

b. HTTP `Range` is served by **vendored `docker/distribution`**
(`blobserver.go:76` `http.ServeContent`) over the `ReadSeeker` the read path
returns — no Kraken change was required to honor `Range:` once `Download`
returned a seekable streaming reader.

c. A separate `agent/agentserver/server.go` `?stream=1` → `streamBlob` branch
(manual 32 KiB Read+Flush loop) was added for a raw blob-endpoint
time-to-first-byte A/B, independent of the registry path soci exercises.

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

> Design only, except one as-built fix: the build-index tag client `Get` send
> timeout was raised **10s → 30s** (`build-index/tagclient/client.go`) because a
> large image's pre-push manifest HEAD triggers a tag lookup that, under
> devcluster push load, transiently exceeded 10s and surfaced as a proxy 500.
> Production must revisit this tag-lookup latency under real load (see Next).

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

1. **P1 — agent-side streaming — DONE.** §2 (streaming reader), §3 (Range via
   the registry read path). soci-snapshotter integrated against the agent in the
   devcluster. Origin stays whole-blob (§5 Phase 1). No tracker/backend change.
2. **P3 — demand-driven fetch — DONE.** §2c lazy mode + demand set: the lazy
   torrent requests only touched pieces (+readahead). This is what produced the
   ~21× byte reduction; folded into P1 for the PoC.
3. **P2 — cold-origin streaming — REMAINING.** §4 (backend range) + §5 Phase 2
   (partial origin seed) + §7 (partial tracker discovery). Plus the `lib/streaming`
   format seam (§ format abstraction) and the build-index dependency resolver
   (§6) for cross-cluster index/data-blob replication.
4. **P4 — compression — REMAINING.** Per-piece zstd, coordinated with the zstd
   workstream.

---

## Next: production-like distributed-cluster PoC

The devcluster PoC ran single-cluster, single-origin, with the index pushed as
an ordinary blob (`soci push --existing-index allow`) discovered via the
`sha256-<digest>` fallback tag. The production-like PoC must exercise what the
devcluster could not:

- **Multi-origin cold streaming.** Validate §4 backend range + §5 Phase 2
  partial origin seed so a cold origin streams ranges from the backend instead of
  materializing the whole blob on first range request.
- **Cross-cluster index/ztoc replication.** Build the §6 build-index dependency
  resolver so the index tag and its referenced data blobs replicate together;
  today only the tag would replicate.
- **Partial-peer discovery on the tracker.** §7 V3 announce carrying progress, so
  cold agents can fetch already-streamed pieces from partial peers, not only
  complete seeders.
- **Tag-lookup latency under real load.** Re-evaluate the 10s→30s tag client
  timeout (§6) against production build-index latencies and large-image pushes.
- **estargz alongside soci.** Push estargz-converted images (client/CI
  conversion) and confirm the format-agnostic range path serves them with no
  Kraken core change (see Format support).

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
