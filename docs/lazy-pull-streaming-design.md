# Lazy-Pull / Image Streaming for Kraken — Design One-Pager

Status: PoC complete (Phase 1 + 3) · Owner: TBD · Date: 2026-06-11

A working end-to-end PoC has shipped on branch `image-streaming` and is
validated in the devcluster against stargz-snapshotter (the original PoC
targeted soci-snapshotter; see [PoC results](#poc-results-2026-06-11) for
both) — see [PoC results](#poc-results-2026-06-11). The sections below mark
what is **as-built** vs. still **design**. **v1 supports stargz only** — see
[Format support](#format-support-estargz).

## Goal

Let a container runtime start before a layer is fully present, by serving
**arbitrary byte ranges of a blob on demand**, with the missing pieces fetched
P2P and prioritized by what the runtime is actually reading. The image format
that describes *which* ranges to fetch (eStargz TOC today, any future format's
chunk index) must be **pluggable** — Kraken should not hardwire to any single
vendor's format, even though v1 ships stargz only.

## Non-goals (v1)

- Re-packing images into a new on-disk format. We keep standard OCI blobs.
- A FUSE filesystem inside Kraken. The runtime-side snapshotter (e.g.
  stargz-snapshotter) stays the FUSE provider; the Kraken **agent is its
  registry**, made range-capable.
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

### eStargz: same win, no Kraken change (2026-06-11)

To confirm the read path is **format-agnostic**, the identical harness was re-run
with **stargz-snapshotter** on eStargz-converted images
(`examples/devcluster/estargz/`), same cold-agent A/B. No Kraken core change —
only the snapshotter and a push-time `nerdctl image convert --estargz`.

| image | snapshotter | overlayfs | lazy | speedup | bytes full | bytes lazy | % fetched | ranged GETs | lazy layers |
|-------|-------------|-----------|------|---------|------------|------------|-----------|-------------|-------------|
| pytorch 2.5.1-cuda12.4-cudnn9-devel | soci | 144.75 s | **13.48 s** | 10.7× | 7071.6 MiB | 325.1 MiB | 4.6% | 239 | 5 |
| pytorch 2.5.1-cuda12.4-cudnn9-devel | estargz | 146.47 s | **16.64 s** | 8.8× | 7096.6 MiB | 295.6 MiB | 4.2% | 553 | 15 |
| python:3.12 (converted) | estargz | 14.86 s | **1.22 s** | 12.2× | 404.6 MiB | 7.4 MiB | 1.8% | 32 | 7 |

estargz matches soci within noise (~9–11× faster, ~4% of bytes, 0 failed
prepares). It fetched slightly fewer bytes (295.6 vs 325.1 MiB) but was slightly
slower (16.64 vs 13.48 s): the finer chunking from `--estargz-min-chunk-size=0`
issues more, smaller ranged GETs (553 vs 239), trading round-trips for
granularity. The 15-vs-5 lazy-layer count is conversion re-layering, not a Kraken
difference. The **only** real asymmetry is push-side: estargz front-loads a heavy
one-time layer recompress (the ~6.9 GiB convert pegged 4+ cores for minutes),
which soci never pays — see "Per-format Kraken changes".

**Harness:** `examples/devcluster/soci/` over a running `make devcluster`. The
host driver `soci_benchmark.sh` builds + starts a privileged DinD container
(containerd + soci-snapshotter + `nerdctl`/`soci`/`ctr`) that runs `run_e2e.sh`
as its workload; results are read back from `docker logs` (deliberately not
`docker exec`, which needs a connection upgrade the busy daemon transiently
refuses). The 206-vs-200 byte split is computed from the agent nginx access logs.

### Agent-to-agent P2P holds while streaming (2026-06-11)

Lazy pull does not bypass Kraken's P2P — a streaming agent still sources pieces
peer-to-peer, not only origin→agent. This is what makes streaming a Kraken
feature rather than a plain registry range read. **No Kraken core change was
needed:** an incomplete agent already serves any piece it holds
(`dispatcher.handlePieceRequest` has no `Complete()` gate), announces each piece
to all peers as it lands (`handlePiecePayload` → `AnnouncePieceMessage`), and the
tracker returns incomplete peers, so a mid-stream agent both serves and
advertises its growing bitfield.

Measured by enabling `network_event` on the agents and tallying agent-two's
`receive_piece` events by source peer: a first agent streams the image cold
(origin-only seeder), then a second agent streams the same content and we count
how many of its pieces came from agent-one (P2P) vs the origin.

| workload | snapshotter | P2P pieces | origin pieces | P2P share |
|----------|-------------|------------|---------------|-----------|
| pytorch 2.5.1-cuda12.4-cudnn9-devel | estargz | 109 | 197 | **35.6%** |
| python:3.12 (converted) | estargz | 17 | 30 | **36.2%** |
| synthetic 512 MiB blob | raw `?stream=1` | — | — | **39.8%** |
| synthetic 256 MiB blob | raw `?stream=1` | — | — | **42.2%** |
| synthetic 64 MiB blob | raw `?stream=1` | — | — | **43.8%** |

Roughly a third to a half of every lazily-streamed image flowed agent→agent while
the snapshotter was still reading it. The share is bounded below full because the
second agent races the first: pieces it demands before agent-one has them (or
advertised them) fall back to the origin — a cold-start effect that tracker
partial-piece discovery (§7) would tighten, not a correctness gap.

**Harness:** `examples/devcluster/p2p_agent_benchmark.sh` for the synthetic
blobs; the estargz figures come from the same `estargz_benchmark.sh` run (its P2P
tally reads each agent's `networkevent.log` via `docker cp`).

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

### Pluggable format abstraction (deferred, format-agnostic seam)

**v1 ships stargz only**, which needs no separate index artifact at all (the
TOC is embedded in each converted layer — see Format support below), so this
seam is not being built now. It is kept here only as the shape a future format
would plug into, without committing to build it:

```go
// lib/streaming/format.go
package streaming

// IndexFormat is a registered streaming-index handler for a format whose
// chunk index lives in a separate artifact from the layer blobs.
type IndexFormat interface {
    // Name is the format key. Used in the derived tag suffix.
    Name() string
    // DependencyDigests parses an index blob and returns the data blobs it
    // references, so build-index can verify + replicate them.
    DependencyDigests(index io.Reader) (core.DigestList, error)
}

// Registry maps format name -> IndexFormat. Populated via Register() at init.
```

See production plan §8 for the deferred cross-cluster index-replication seam
this abstraction exists to support, if a future format ever needs it.

> **Status:** design only, deferred. The PoC did **not** build `lib/streaming`
> (grep: zero `IndexFormat`/`DependencyDigests` hits). The original PoC ran
> against soci-snapshotter, whose index was pushed as an ordinary blob via
> `soci push --existing-index allow` and discovered through a derived tag —
> single cluster, so no dependency resolver was needed even then. v1 (stargz)
> needs no index blob at all.

### Format support: estargz

The byte-level read path is **format-agnostic**: any snapshotter that issues
ranged GETs on layer blobs is served by the same streaming reader. v1 supports
one format:

| format | chunk index | image conversion | discovery | what Kraken must add |
|--------|-------------|------------------|-----------|----------------------|
| **estargz** | embedded as a TOC footer **inside each layer blob** | **required** (`nerdctl image convert --estargz` / `ctr-remote`) | implicit — no separate artifact, no referrers | **nothing in core** — converted layers are opaque blobs; range reads suffice. |

Key consequence: **estargz is supported by the PoC with no Kraken change** —
Kraken stores/serves blobs opaquely by digest (`rw_transferer.Upload` ignores
media type), so estargz-converted layers push through unchanged and the same
range path serves them. The only cost is push-time conversion, a client/CI
concern. (`--estargz-external-toc` moves the TOC into a separate "TOC image"
pushed to the same registry — still just blobs + tags, still no referrers.)

A format whose chunk index lives in a *separate* artifact (rather than
embedded in the layer, as stargz does) would additionally need the derived-tag
discovery and dependency-resolver mechanism the original PoC exercised with
soci — see production plan §8 for why that is deferred rather than built now.

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

`docs/lazy-pull-streaming-critical-review.md` independently confirmed this
priority mechanism is a single ascending-sorted map with no per-stream or
"currently blocked piece" classification, and flagged a real starvation risk:
a reader blocked on a high-index piece can lose pipeline slots to another
stream's lower-index demand, or to stale readahead. See production plan §12
for the resulting recommendation.

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

### 6. Proxy + build-index — distribute & discover the index (deferred, format-agnostic)

> Design only, except one as-built fix: the build-index tag client `Get` send
> timeout was raised **10s → 30s** (`build-index/tagclient/client.go`) because a
> large image's pre-push manifest HEAD triggers a tag lookup that, under
> devcluster push load, transiently exceeded 10s and surfaced as a proxy 500.
> Production must revisit this tag-lookup latency under real load (see Next).

**Not needed for v1 (stargz has no separate index blob)** — kept here only as
the shape a future format with a separate index artifact would need. See
production plan §8.

- Proxy push path is unchanged: an index blob (if a format has one) would ride
  the existing `Upload` (`rw_transferer.go:193`) and its derived tag would ride
  `PutTag`/`PutAndReplicate` (`rw_transferer.go:234`).
- **New dependency resolver** in `build-index/tagtype/`: register a resolver
  (alongside `"docker"`/`"default"` in `map.go:70`) for that format's tag
  namespace that calls `streaming.Registry[format].DependencyDigests(...)`.
  This would make build-index (a) verify the index's data blobs exist on
  origin before accepting the tag (`tagserver/server.go:513`) and (b)
  replicate them cross-cluster (`:569`,
  `tagreplication.NewTask(tag, d, deps, ...)`). Without this the index tag
  replicates but its referenced blobs might not.

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

## Per-format Kraken changes: estargz (exact)

The streaming read path is format-agnostic — a format whose chunk index rides
inside the layer blob (like estargz) needs nothing extra; a format with a
separate index artifact would need cross-cluster replication support, which is
why that seam is kept pluggable (see Format support above and production plan
§8) even though nothing implements it today. The **shared core is the same
regardless of format** and is already built (§2 streaming reader, §2c
demand-driven fetch, §2d in-order policy, §3 Range via the registry read path).

### estargz — Kraken core changes: none

estargz embeds its TOC inside each layer blob, so there is nothing extra for
Kraken to store, discover, or replicate:

- **Storage / serve:** unchanged. `rw_transferer.Upload` (`rw_transferer.go:193`)
  is media-type-agnostic — a converted layer is an opaque blob keyed by digest,
  served by the same Range path.
- **Discovery:** none. The snapshotter reads the TOC from the layer itself; no
  derived tag, no resolver, no Referrers.
- **Cross-cluster:** none. The TOC rides inside the layer blob, which already
  replicates as part of the image.
- **Push-time conversion** (`nerdctl image convert --estargz`) is a client/CI
  step, not a Kraken change.
- **Optional only:** an `estargz` `IndexFormat` in `lib/streaming` (§ format
  abstraction) if build-index should *understand* the format — but estargz has no
  separate artifact to resolve, so this is unnecessary for correctness.

Net: estargz already works on the as-built PoC, verified end-to-end by
`examples/devcluster/estargz/` (7 lazy layers, 7.4 MiB vs 404.6 MiB, 0 failures).

A format whose index lives in a separate artifact instead (as the original PoC
target, soci, did) would additionally need a cross-cluster dependency resolver
so that index replicates together with its data blobs — see production plan §8
for why that seam is deferred rather than built, since v1 only needs estargz.

---

## zstd interaction (do this right or it breaks streaming)

### Current state: Kraken is compression-agnostic

There is **no zstd code or design in the repo today** (grep: zero `zstd`,
`klauspost`, `seekable` in Go source; zero compression libraries in `go.mod`).
The entire blob pipeline — push, store, piece-hash, P2P transfer, serve —
operates on **opaque bytes** with zero decompression:

- **Digest**: SHA256 of raw bytes (`core/digester.go:36`)
- **Piece sums**: CRC32-IEEE of raw bytes (`core/piece_hash.go:21`,
  `core/metainfo.go:145` — `io.CopyN(h, blob, pieceLength)`)
- **Storage**: CAStore writes bytes as-is (`lib/store/ca_store.go:183`)
- **Serve**: `io.Copy(w, f)` — raw bytes to wire (`agent/agentserver/server.go:175`)
- **Media types**: parsed for manifest routing only, never for content
  transformation (`utils/dockerutil/dockerutil.go:35-76`)

This means Kraken **already accepts `tar+zstd` layers** — if a client pushes
them, they are stored, piece-hashed, P2P-distributed, and served back
identically to `tar+gzip`. The client (containerd 1.5+) handles decompression.
The only gate is whether the vendored `docker/distribution` v2.7.1 rejects
zstd media types during manifest validation (the OCI `+zstd` media type was
added in OCI Image Spec v1.1.0, February 2024).

### The hard rule for streaming compatibility

Piece CRCs are over the **stored bytes** (`core/metainfo.go` `calcPieceSums`),
and piece reads are linear seeks `offset = piece * pieceLength`
(`storage/piecereader/file.go:96`). **Whole-blob single-stream zstd destroys
this 1:1 offset mapping and is incompatible with byte-range / lazy pull.**

### Three levels of zstd support

**Level 1 — Accept zstd layers (zero Kraken changes).** Already works. The
blob is opaque. The only prerequisite: ensure the manifest validator accepts
`application/vnd.oci.image.layer.v1.tar+zstd` (OCI v1.1.0). PR #584 (open,
OCI manifest/index support) is adjacent.

**Level 2 — At-rest / transport compression (per-piece zstd).** Compress each
`PieceLength` chunk as an independent zstd frame for storage savings and P2P
bandwidth reduction. This is the recommended production path.

**Level 3 — Seekable zstd for lazy pull.** The zstd seekable format (RFC 8878
skippable frames + seek table) enables random-access decompression — relevant
only if Kraken needs to serve decompressed bytes from a compressed at-rest
store, which it currently does not (the client decompresses).

### Per-piece zstd design (Level 2 — recommended)

Each `PieceLength` chunk is an independent zstd frame. Frame boundary == piece
boundary, so offset math is preserved.

**How it works:**

1. At writeback (or upload commit), compress each piece independently:
   ```
   for each piece [i]:
     compressed[i] = zstd.Encode(blob[i*pieceLen : (i+1)*pieceLen])
   ```
2. CRC32 piece sums are computed over the **compressed** bytes (same principle
   as today — piece sums are over whatever bytes are on disk/wire).
3. The piece reader decompresses on read:
   ```
   GetPieceReader(i) → zstd.Decode(compressed[i])
   ```
4. The metainfo gains a `Compressed bool` or codec field so peers know to
   decompress. The infohash changes (piece sums are over different bytes), so
   compressed and uncompressed torrents are distinct swarms — no mixed-mode
   ambiguity.

**Compression ratio tradeoff:**

| Method | Ratio (typical container layer) | Random access? |
|--------|--------------------------------|----------------|
| Solid zstd (whole blob) | ~3.0–3.2× | No |
| Per-piece zstd (4 MiB pieces) | ~2.5–3.0× | Yes |
| Per-file zstd (eStargz-zstd) | ~2.0–2.8× | Yes (file-level) |
| gzip (current OCI default) | ~2.0–2.5× | No |

The ~10–15% ratio penalty vs solid compression comes from lost cross-piece
dictionary context and per-frame overhead. It shrinks with larger pieces and
is more than offset by the bandwidth savings from demand-driven fetch (only
touched pieces transferred).

**Go libraries:** `github.com/klauspost/compress/zstd` (pure Go, no cgo,
used by containerd, CockroachDB) + `github.com/SaveTheRbtz/zstd-seekable-format-go`
(implements full seekable format spec, wraps klauspost).

**What changes in Kraken:**

- `core/metainfo.go`: add `Codec` field to `info` struct (default empty =
  uncompressed, `"zstd"` = per-piece zstd). Must be included in the bencoded
  `info` so it affects the infohash (compressed != uncompressed swarm).
- `lib/metainfogen/generator.go`: optionally compress each piece before
  computing CRC32, store compressed length per piece (variable-length pieces).
- `lib/torrent/storage/piecereader/`: decompress on read when codec is set.
- `lib/torrent/storage/agentstorage/torrent.go`: `WritePiece` accepts
  compressed bytes, `GetPieceReader` decompresses.
- **Piece length becomes variable** after compression. The metainfo currently
  assumes uniform piece length (`PieceLength(0)` for all but the last). With
  per-piece compression, each piece has a different compressed size. Two options:
  (a) store compressed sizes in metainfo (cleanest, ~4B/piece overhead), or
  (b) pad each compressed frame to the original piece length (wastes space,
  preserves uniform offset math). Recommend (a).
- P2P protocol: no change needed — `PiecePayloadMessage` already carries
  arbitrary bytes. Peers decompress locally after CRC32 verification.

**OCI digest interaction:** OCI layer digests are over **compressed** bytes
(manifest `layers[].digest` = SHA256 of the blob as stored). OCI DiffIDs
(image config `rootfs.diff_ids`) are over **uncompressed** tar bytes. Kraken's
blob digest (`core/digest.go`) is the OCI layer digest (compressed bytes), so
per-piece zstd does NOT change the blob identity — it changes the internal
piece representation. The blob digest stays the SHA256 of the original
(pre-Kraken-compression) bytes. Kraken's at-rest compression is a transport
optimization, invisible to OCI.

### eStargz-zstd (zstdchunked) — already works with streaming

stargz-snapshotter fully supports zstd as an alternative to gzip. The format
(`zstdchunked`) uses independent zstd frames per file chunk and a TOC in a
zstd skippable frame. Key details:

- **Footer**: 40 bytes (vs 51 for gzip-eStargz). Magic: `GnUlInUx` (8 bytes).
  Encodes TOC offset/compressed-size/uncompressed-size as uint64 LE.
- **TOC**: JSON compressed with zstd, stored in a skippable frame
  (`0x184D2A5E` magic). Same `TOCEntry` structure as gzip-eStargz (file
  metadata + `Offset`/`ChunkOffset`/`ChunkDigest` per chunk).
- **DiffID**: preserved (TOC is in skippable frames, not in the tar stream —
  unlike gzip-eStargz which embeds `stargz.index.json` as a tar entry).
- **Media type**: standard `application/vnd.oci.image.layer.v1.tar+zstd`.
- **OCI annotations**: `io.containers.zstd-chunked.manifest-position` =
  `tocOffset:compressedLen:rawLen:manifestType`.

For Kraken, eStargz-zstd layers are opaque blobs served via the same
format-agnostic range path. No Kraken change required — confirmed by the PoC
(gzip-eStargz already validated; zstd variant is structurally identical from
Kraken's perspective).

### Recommendation

1. **v1 (now):** do nothing. Kraken already accepts zstd layers. The streaming
   read path is format-agnostic. eStargz-zstd works out of the box.
2. **v2 (coordinate with zstd workstream):** per-piece zstd for P2P transport
   bandwidth reduction. Land it as an opt-in codec in metainfo, behind a
   config flag. ~200–300 LOC (codec in metainfo, compress at write, decompress
   at read, variable piece sizes). Flag this constraint to the zstd effort now:
   **per-piece, not whole-blob**.

---

## Phasing

1. **P1 — agent-side streaming — DONE.** §2 (streaming reader), §3 (Range via
   the registry read path). soci-snapshotter integrated against the agent in the
   devcluster. Origin stays whole-blob (§5 Phase 1). No tracker/backend change.
2. **P3 — demand-driven fetch — DONE.** §2c lazy mode + demand set: the lazy
   torrent requests only touched pieces (+readahead). This is what produced the
   ~21× byte reduction; folded into P1 for the PoC.
3. **P2 — cold-origin streaming — IMPLEMENTED IN PLAN, NOT FLEET-READY (Stack
   B).** §4 (backend range) + §5 Phase 2 (partial origin seed) designed — see
   production plan for the as-built detail and its §12 observability gap
   before calling this fleet-ready. §7 (partial tracker discovery) remains.
   The `lib/streaming` format seam (§ format abstraction) and the §6
   build-index dependency resolver are **deferred** — v1 (stargz) needs no
   separate index blob, so cross-cluster index/data-blob replication has
   nothing to replicate today (production plan §8).
4. **P4 — compression — REMAINING.** Per-piece zstd, coordinated with the zstd
   workstream.

---

## Next: production-like distributed-cluster PoC

The devcluster PoC ran single-cluster, single-origin. The original PoC target
(soci-snapshotter) discovered its index via an ordinary blob pushed with
`soci push --existing-index allow` and the `sha256-<digest>` fallback tag; v1
(stargz) needs no such index at all. The production-like PoC must exercise
what the devcluster could not:

- **Multi-origin cold streaming.** Validate §4 backend range + §5 Phase 2
  partial origin seed so a cold origin streams ranges from the backend instead of
  materializing the whole blob on first range request.
- **Partial-peer discovery on the tracker.** §7 V3 announce carrying progress, so
  cold agents can fetch already-streamed pieces from partial peers, not only
  complete seeders.
- **Tag-lookup latency under real load.** Re-evaluate the 10s→30s tag client
  timeout (§6) against production build-index latencies and large-image pushes.
- **Cross-cluster index replication** stays deferred (§6, production plan §8)
  until a format that actually needs a separate index blob is adopted.

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
3. Streaming: feature branch, stargz-snapshotter cold pull ×N (image
   pre-converted; stargz needs no separate index push).
4. Compare TTFB, total, bytes-fetched-before-start, and P2P leech throughput
   from the observability histograms. Expect large TTFB win, neutral-to-slightly
   higher total bytes (range overhead), no regression on the non-streaming path.

---

## Runtime integration (no containerd/dockerd core changes)

- **containerd:** reuse the existing remote-snapshotter API. Run
  stargz-snapshotter pointed at the **agent** as its registry
  (`localhost`). Pure config (register snapshotter, CRI `snapshotter` field).
- **dockerd:** out of scope — lazy pull requires containerd's snapshotter API;
  classic dockerd graphdriver hooking is not worth it. Target containerd/k8s.

## Open questions

1. **Piece length vs. snapshotter chunk size (resolved).**
   eStargz chunks won't align with Kraken `PieceLength` (size-bucketed,
   `lib/metainfogen/config.go:70`). Every range read rounds to whole-piece
   boundaries because the agent verifies each piece via CRC32 before serving
   any byte (`agentstorage/torrent.go:192`). This is the load-bearing
   constraint — it cannot change without breaking P2P integrity.

   **Read amplification:** `bytes fetched = (distinct pieces touched) × pieceLen`.
   For sequential workloads (e.g. `import torch`) deduplication keeps aggregate
   amplification low (~4.2% measured). For sparse random access the worst case
   per-read is `pieceLen / readSize` (up to 16–32× for small reads against
   4 MiB pieces).

   **Resolution — byte-budgeted, sequential-only readahead:**
   The current `streamReader` applies a fixed 8-piece readahead
   (`streamReadahead = 8`) in `acquirePiece` on *every* blocking piece,
   including random `ReadAt` calls. `ReadAt` already pre-demands its exact
   span (`:166`), so the readahead overshoot on random reads is pure waste.
   The fix:
   - Replace `const streamReadahead = 8` (pieces) with a byte-budget constant
     `streamReadaheadBytes` (default 32 MiB, same effective budget as today for
     4 MiB pieces, but adapts to other piece sizes).
   - Add a struct field `readahead int` = `budget / pieceLen` (min 1, 0 for
     empty blobs), computed once at construction.
   - Change `acquirePiece(piece, readahead int)` so demand becomes
     `[piece, piece+readahead+1)`.
   - `openAt` (sequential `Read`) passes `r.readahead` — full prefetch window.
   - `ReadAt` (random) passes `0` — keeps the `priority()` hint so the
     dispatcher knows which piece to fetch next, but no overshoot beyond
     the exact span already demanded.
   This pays twice: P2P transfer *and* origin→backend egress (Stack B cold
   origin range-fetch). No change to the verification model or piece grid.
