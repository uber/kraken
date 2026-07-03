# Lazy-pull / image-streaming: production implementation plan

Status: proposal. Companion to [lazy-pull-streaming-design.md](lazy-pull-streaming-design.md)
(the design + PoC results). This doc turns the proven PoC into a **stack of small,
independently-mergeable PRs** — every PR changes **≤150 lines of non-test code**,
compiles on its own, and is inert until a later PR activates it. Reviewed
against [lazy-pull-streaming-critical-review.md](lazy-pull-streaming-critical-review.md);
see §12 for the resulting corrections.

## 1. Strategy

The PoC on branch `image-streaming-p2p` already implements a **correct,
P2P-functional** agent-side streaming read path (measured 8–12× faster
time-to-running, ~4% of bytes fetched, 35–44% of streamed pieces sourced
agent↔agent — see the design doc's PoC results). So production is **not a
rewrite**. It is:

1. **Re-slice** the ~580 lines of as-built PoC core into a reviewable stack.
2. **Drop the devcluster-only instrumentation** the PoC carried (networkevent
   wiring, `?stream=1` A/B endpoint, mutex-profile flags, ad-hoc counters).
3. **Build the genuinely-new production pieces** the PoC skipped, as their own
   later stacks: cold-origin range streaming, tracker partial-aware discovery,
   per-piece zstd. A generic, format-agnostic index-replication seam
   (`lib/streaming` + resolver) is kept as a deferred extension point, not
   built now — v1 supports stargz only, which needs no separate index blob.

The whole of **Stack A** (the agent-side streaming path) is what makes a runtime
snapshotter lazily pull a real image through a single Kraken cluster. **Stacks
B and D are required for production** — the fleet has cold (multi-host) origins
and cold-start swarms — and so get the same declaration-level detail below
(§7, §9). **Stack C** (§8) is deferred: format-agnostic by design, but not
needed until a lazy format that ships a separate index artifact is adopted.
**Stack E** (per-piece zstd) is deferred to a separate, coordinated workstream
(§10). None of B–D is needed for single-cluster *correctness* (the design doc
establishes this for §5/§7), but B and D are needed before the streaming path
can ship to the production fleet.

### Reuse posture

Almost every surface the streaming path needs **already exists on `master`** and
is reused unchanged:

| Need | Reused as-is (master) |
|---|---|
| Read pieces as they land | `storage.Torrent` — `HasPiece` / `GetPieceReader` / `PieceLength` / `Length` / `Complete` (`lib/torrent/storage/storage.go:38`) |
| Serve partial pieces to peers | `Dispatcher.handlePieceRequest` — **no `Complete()` gate** (`dispatch/dispatcher.go:545`) |
| Announce each new piece | `handlePiecePayload` → `conn.NewAnnouncePieceMessage` (`dispatch/dispatcher.go:577`, `conn/message.go:79`) |
| Piece-selection plug point | `pieceSelectionPolicy` + `NewManager` switch + `piece_request_policy` key (`piecerequest/policy.go:25`, `manager.go:73`, `dispatch/config.go:37`) |
| Docker-registry read path | `ImageTransferer.Download` returns `store.FileReader` (`transfer/transferer.go:24`) — **no interface change** |
| HTTP Range to the snapshotter | vendored `docker/distribution` `http.ServeContent` over the seekable reader (`blobserver.go:76`) |
| Schedule / config / metrics | `Scheduler` (`scheduler.go:50`), `scheduler.Config` (`config.go:26`), `tally.Scope` + `observability.EmitDownloadPerformance` (`scheduler.go:291`) |

The only **new public surface** is one interface (`BlobReader`) and one method
(`Scheduler.DownloadReader`). Everything else is internal to the scheduler
package.

## 2. References

- Design + PoC results: [lazy-pull-streaming-design.md](lazy-pull-streaming-design.md)
- PoC commits on `image-streaming-p2p` (source to lift from, oldest→newest):
  - `5df4d96d` phase 1 — agent streaming reader + registry read path
  - `59f5ce8e` phase 2 — soci e2e
  - `22d8dcc8` phase 3 — demand-driven (lazy) fetch
  - `a6506984` estargz format run; `338663b8` nydus; `e48783ea` p2p verification
- As-built diff: `git diff master...image-streaming-p2p -- lib/torrent lib/dockerregistry`

## 3. Design principles for the stack

- **≤150 non-test LOC per PR.** Tests are unbounded and expected with each PR.
- **Inert until activated.** Each PR is a no-op for existing behavior until a
  later PR calls into it (e.g. `SetLazy` exists three PRs before anything calls
  it). This keeps every intermediate merge safe to ship and roll back.
- **No interface break mid-stack.** The streaming reader's constructor takes its
  final signature in the PR that introduces it; later PRs fill in behavior, not
  shape. Avoids call-site churn across PRs.
- **Reuse over re-add.** Where master already has the symbol, the PR wires to it;
  it does not reintroduce it.

## 4. Clean interfaces (consolidated)

These are the production interfaces the stack lands. Signatures match the
reusable master surfaces so the diffs stay minimal.

### 4.1 `BlobReader` — the streaming read handle (new, scheduler package)

```go
// lib/torrent/scheduler/scheduler.go
//
// BlobReader serves a blob that may still be downloading: reads block per-piece
// (not per-blob) until the covering pieces land. Satisfies store.FileReader and
// io.ReaderAt so the docker-registry read path's http.ServeContent can range it.
type BlobReader interface {
    io.ReadSeekCloser
    io.ReaderAt
    Size() int64 // full blob length from metainfo, known before any byte lands
}
```

### 4.2 `Scheduler.DownloadReader` — the entry point (new method)

```go
// Added to the existing Scheduler interface (scheduler.go:50). Non-blocking:
// returns a reader over the live torrent instead of blocking until Complete().
DownloadReader(namespace string, d core.Digest) (BlobReader, error)
```

Reuses `torrentArchive.CreateTorrent` and the event loop exactly as `Download`
does; the only difference is it returns a reader instead of `<-errc`-blocking.

### 4.3 Internal APIs

The internal dispatcher (`SetLazy`/`RequestPieces`/`SetPriorityPiece`) and
`piecerequest` (`Manager.SetPriority`) APIs are detailed per-PR in §5.3 (A1–A3).

## 5. Stack A — agent-side streaming (v1)

This is the deliverable that makes single-cluster lazy pull work. Ten PRs,
each ≤150 non-test LOC. Lift each from the as-built PoC, dropping instrumentation.

### 5.1 Dependency order

```
A1 manager priority ──────────▶ A3 dispatcher demand/priority API
A2 dispatcher lazy state ─────▶ A3
A3 ─▶ A4 scheduler events (eager) ─▶ A6 DownloadReader+mocks ─▶ A9 registry path
A5 stream reader (eager) ─────────▶ A6
A6 + A5 ─▶ A7 lazy activation (demand+readahead+SetLazy) ─▶ A8 ReadAt ─▶ A9
A10 config + cleanup (last)
```

### 5.2 PR budget table

| PR | Scope | Files | ~LOC (non-test) | Activates? |
|----|-------|-------|------|-----------|
| A1 | manager priority reservation | `manager.go` | ~47 | inert until A3 calls `SetPriority` |
| A2 | dispatcher lazy-demand state | `dispatcher.go` | ~36 | inert until A7 calls `SetLazy` |
| A3 | dispatcher demand/priority API | `dispatcher.go` | ~26 | inert until A4/A7 wire it |
| A4 | scheduler streaming events (eager) | `state.go` + `events.go` | ~52 | reached by A6 |
| A5 | streaming reader — sequential, eager | `stream_reader.go` (new) | ~150 | unused until A6 |
| A6 | `DownloadReader` + `BlobReader` + mocks | `scheduler.go` + `mocks/...` | ~33 (+~33 generated mocks) | **TTFB win live (eager)** |
| A7 | lazy activation — demand + readahead | `stream_reader.go` + `events.go` (1 line) | ~33 | **byte-savings live (lazy)** |
| A8 | streaming reader — `ReadAt` (range) | `stream_reader.go` | ~43 | range reads |
| A9 | registry read path uses streaming | `ro_transferer.go` | ~16 | **snapshotter streams via registry** |
| A10 | PoC cleanup (deletions only) | remove instrumentation | net ~−45 (deletions) | drops PoC-only glue, no policy/config change |

LOC are verified against the as-built code at HEAD on `image-streaming-p2p`. Each
PR's own non-test delta is ≤150; the §5.3 detail lists exact declarations per PR.

Two natural seams keep the big files under budget: `stream_reader.go` (293 in the
PoC) splits across **A5 / A7 / A8**; `dispatcher.go` (95) splits across **A2 / A3**.

### 5.3 PR detail

Each PR below lists its files, imports, the exact Go **declarations** it adds or
changes (signatures + a short `// body:` note — not full implementations), the
call-site edits, and the tests. Signatures, field names, and const values are
verbatim from the as-built PoC at HEAD.

---

#### A1 — manager priority reservation

**Files:** `manager.go`; `manager_test.go`. **Imports added:** none.

```go
// New field on the existing Manager struct (after originPipelineLimit int):
	// priority holds pieces a streaming reader is blocked on; reserved ahead
	// of the selection policy.
	priority map[int]struct{}

// SetPriority marks piece i to be reserved ahead of the policy.
func (m *Manager) SetPriority(i int)
// body: Lock/defer Unlock; m.priority[i] = struct{}{}.

// sortedPriority returns priority pieces ascending. Caller holds the lock.
func (m *Manager) sortedPriority() []int
// body: collect keys, sort.Ints, return.

// ReservePieces — signature UNCHANGED; body reworked to reserve priority first.
func (m *Manager) ReservePieces(
	peerID core.PeerID,
	isPeerOrigin bool,
	pieceCandidates *bitset.BitSet,
	numPeersByPiece syncutil.Counters,
	allowDuplicates bool) ([]int, error)
// body: if len(m.priority)>0, walk sortedPriority() appending each i that is a
// candidate && valid (tracked in `chosen`) until quota; fill the remainder via
// policy.selectPieces(quota-len, policyValid, ...) excluding chosen; if nothing
// chosen, fall back to the original policy.selectPieces(quota, valid, ...).
// Pending bookkeeping (requests/requestsByPeer) unchanged.

// Clear — gains one line to drop the priority hint on completion.
func (m *Manager) Clear(i int)
// body: existing delete(m.requests, i) + new delete(m.priority, i) + existing
// requestsByPeer cleanup.
```

**Call-site edits:** `NewManager` struct literal gains
`priority: make(map[int]struct{}),`; `ReservePieces` block rewritten as above;
`Clear` gains `delete(m.priority, i)`.
**Tests:** `TestManagerSetPriority` (table: priority ahead of rarest-first,
priority then policy fills quota, multiple priorities ascending, non-candidate
priority falls back); `TestManagerClearRemovesPriority`.
**LOC (non-test):** ~47.

---

#### A2 — dispatcher lazy-demand state (inert)

**Files:** `dispatch/dispatcher.go`; `dispatcher_test.go`. **Imports added:** none
(`sync`, `bitset`, `fmt` already imported on master).

```go
// New fields on Dispatcher (after torrentlog *torrentlog.Logger):
	// demandMu guards lazy and demand. When lazy, only pieces in demand are
	// requested; default (lazy false, demand nil) is master eager behavior.
	demandMu sync.Mutex
	lazy     bool
	demand   *bitset.BitSet

// SetLazy switches to demand-driven fetching. Idempotent.
func (d *Dispatcher) SetLazy()
// body: lock; return if already lazy; set lazy=true,
// demand=bitset.New(uint(d.torrent.NumPieces())).

// restrictToDemand intersects candidates with demand when lazy; passthrough else.
func (d *Dispatcher) restrictToDemand(candidates *bitset.BitSet) *bitset.BitSet
// body: lock; if !lazy || demand==nil return candidates;
// else return candidates.Intersection(d.demand).
```

**Call-site edits (no-ops while `SetLazy` is never called):**
- `maybeRequestMorePieces`: after `candidates := p.bitfield.Intersection(d.torrent.Bitfield().Complement())`,
  insert `candidates = d.restrictToDemand(candidates)`.
- `resendFailedPieceRequests`: change `if candidates.Test(uint(r.Piece))` to
  `if d.restrictToDemand(candidates).Test(uint(r.Piece))`.

**Tests:** `TestDispatcherEagerRequestsAllMissingPieces` (no `SetLazy` → all
missing requested == master); `TestDispatcherLazyRequestsOnlyDemandedPieces`
(`SetLazy`, empty demand → none; `demand.Set(2)` → only piece 2).
**LOC (non-test):** ~36.

---

#### A3 — dispatcher demand/priority API

**Files:** `dispatch/dispatcher.go`; `dispatcher_test.go`. **Imports added:** none.
**Depends on:** A1 (`Manager.SetPriority`) and A2 (demand fields) — stack A1→A2→A3.

```go
// SetPriorityPiece hints a piece be requested ahead of the policy.
func (d *Dispatcher) SetPriorityPiece(piece int)
// body: d.pieceRequestManager.SetPriority(piece) — Manager.SetPriority from A1.

// RequestPieces marks pieces as demanded and kicks a request round. Only
// meaningful in lazy mode; the first piece is also prioritized.
func (d *Dispatcher) RequestPieces(pieces []int)
// body: return if empty; under demandMu set each bit on d.demand (when non-nil);
// SetPriorityPiece(pieces[0]); d.peers.Range (panic on non-*peer as elsewhere),
// per peer `go d.maybeRequestMorePieces(p)` logging errors via d.log.
```

**Call-site edits:** none — these are new entry points called by the streaming
reader (via the `streamResult` callbacks bound in A4), not wired into existing
dispatch control flow.
**Instrumentation excluded** (kept out of the core PR; final disposition in §6):
`lazy_pieces_requested` counter, `demandCount()`, the teardown demand log.
**Tests:** `TestDispatcherSetPriorityPiece` (priority piece reserved ahead of
policy end-to-end through A1); `TestDispatcherRequestPieces` (table: eager
request is demand-noop, lazy single, lazy multiple, lazy empty noop).
**LOC (non-test):** ~26. (A2+A3 ≈ 62, vs ~95 in the PoC once instrumentation drops.)

---

#### A4 — scheduler streaming events (eager)

**Files:** `scheduler/state.go`; `scheduler/events.go`. **Imports added:** none
(both already import `storage`).

```go
// state.go — new field on torrentControl (after localRequest bool):
	// torrent is the live storage.Torrent the dispatcher writes into; streaming
	// readers must use this instance so HasPiece observes pieces as they land.
	torrent storage.Torrent

// events.go
type streamResult struct {
	torrent  storage.Torrent    // live torrent; nil signals add failure
	errc     chan error         // buffered(1); terminal download state
	priority func(piece int)    // bound to dispatcher.SetPriorityPiece (A3)
	request  func(pieces []int) // bound to dispatcher.RequestPieces (A3)
}

type streamTorrentEvent struct {
	namespace string
	torrent   storage.Torrent   // created by DownloadReader (A6)
	result    chan streamResult // buffered(1)
}

// apply begins leeching (if not already) and hands back the live torrent + an
// errc registered for terminal-state signaling. EAGER: does NOT call SetLazy.
func (e streamTorrentEvent) apply(s *state)
// body: lookup s.torrentControls[infohash]; if absent s.addTorrent(namespace,
// torrent, true) — on err send streamResult{errc: errcWith(err)}, return.
// errc=make(chan error,1); if dispatcher.Complete() push nil else append to
// ctrl.errors and `go s.sched.announce(...)`. Send streamResult{ctrl.torrent,
// errc, priority: dispatcher.SetPriorityPiece, request: dispatcher.RequestPieces}.

func errcWith(err error) chan error
// body: errc:=make(chan error,1); errc<-err; return errc.
```

**Call-site edit:** `addTorrent` sets `torrent: t,` in the `torrentControl` literal.
**Deferred to A7:** the single line `ctrl.dispatcher.SetLazy()` in `apply`'s
new-torrent branch is **withheld here** (eager mode in A4).
**Tests:** `TestStreamTorrentEventAddsTorrentAndReturnsLiveTorrent`,
`...ReturnsCompleteImmediately`, `...AddTorrentErrorReturnsErrc`, `TestErrcWith`.
**LOC (non-test):** ~52. No mock regen (no interface change).

---

#### A5 — streaming reader, sequential + eager

**Files:** `scheduler/stream_reader.go` (new). **Imports added:** `fmt`, `io`,
`time`, `clock`, `lib/torrent/storage`, `utils/closers`.

> The struct field set and `newStreamReader` signature are **final in A5** so A7/A8
> add behavior, not shape (no call-site churn). `priority`/`request`/`hinted` are
> stored but **unused** in A5; `acquirePiece` is poll-only here, upgraded in A7.

```go
package scheduler

// streamPollInterval is the wait between checks for the next piece.
const streamPollInterval = 5 * time.Millisecond

// streamReader serves a torrent's bytes while it is still downloading, blocking
// only on the piece a read needs. Shares the dispatcher's live torrent. Implements
// store.FileReader (Read/ReadAt/Seek/Close/Size) for http.ServeContent ranging.
// Read/Seek are stateful (not concurrent-safe); ReadAt is cursor-independent.
type streamReader struct {
	t            storage.Torrent     // live dispatcher torrent
	errc         chan error          // terminal state (nil=complete, else err)
	clk          clock.Clock         // injected for deterministic test backoff
	pollInterval time.Duration       // backoff between availability polls
	priority     func(piece int)     // FINAL; UNUSED in A5 (wired in A7)
	request      func(pieces []int)  // FINAL; UNUSED in A5 (used by demand() A7)

	length   int64 // t.Length()
	pieceLen int64 // PieceLength(0); 0 for empty blobs

	pos    int64               // next sequential Read position
	pr     storage.PieceReader // currently open piece reader, if any
	prOff  int64               // absolute position pr is at
	hinted int                 // FINAL; UNUSED in A5 (priority guard A7); -1=none

	done    bool  // terminal state received
	termErr error // terminal download error
}

// newStreamReader — FINAL signature. priority/request may be nil (eager).
func newStreamReader(
	t storage.Torrent,
	errc chan error,
	clk clock.Clock,
	pollInterval time.Duration,
	priority func(piece int),
	request func(pieces []int)) *streamReader
// body: pieceLen = t.PieceLength(0) iff NumPieces()>0 else 0; return &streamReader{..., hinted:-1}.

func (r *streamReader) Size() int64                         // return r.length
func (r *streamReader) Read(p []byte) (int, error)          // open/advance piece, block on missing
func (r *streamReader) Seek(offset int64, whence int) (int64, error) // resolve abs; err on bad whence/neg
func (r *streamReader) openAt(pos int64) error              // acquirePiece + discard intra-piece offset
func (r *streamReader) acquirePiece(piece int) (storage.PieceReader, error) // A5: poll-only (no priority/demand)
func (r *streamReader) waitPiece() error                    // select errc vs clk.After(pollInterval)
func (r *streamReader) Close() error                        // close r.pr if open
```

**Tests:** drive against a real `*agentstorage.Torrent` whose pieces are released
on a schedule by a goroutine calling `WritePiece` with lag (the "fake that
releases on a schedule"): `TestStreamReaderServesPiecesAsTheyArrive`,
`...HandlesAlreadyCompleteTorrent`, `...ReturnsTerminalError`; Seek/EOF ride on
`io.ReadAll`.
**LOC (non-test):** ~150 (the constrained PR).

> Reader-signal note: the PoC **polls** `HasPiece` at 5 ms rather than a
> `WritePiece` fan-out (`sync.Cond`/channel). Polling is simple and adequate; the
> signal-based fan-out is a future optimization if per-uncached-piece latency
> matters at scale. v1 keeps polling.

---

#### A6 — `DownloadReader` + `BlobReader` + mocks

Wires the reader from **A5** into the events from **A4** — merges after both. This
is the PR that makes an **eager** streaming reader reachable end-to-end (TTFB win,
no byte-savings yet; lazy comes in A7).

**Files:** `scheduler/scheduler.go`; `mocks/lib/torrent/scheduler/scheduler.go`
and `reloadablescheduler.go` (generated). **Imports added:** `io` in
`scheduler.go`; `scheduler` package import in the mock (mockgen byproduct).

```go
// scheduler.go — new interface above Scheduler:
// BlobReader serves a blob that may still be downloading; satisfies
// store.FileReader + io.ReaderAt so http.ServeContent can range it.
type BlobReader interface {
	io.ReadSeekCloser
	io.ReaderAt
	Size() int64
}

// Added to the Scheduler interface (after the existing Download line):
	DownloadReader(namespace string, d core.Digest) (BlobReader, error)

// DownloadReader schedules a download and returns a reader that serves bytes in
// order as pieces arrive, without waiting for the whole blob.
func (s *scheduler) DownloadReader(
	namespace string, d core.Digest) (BlobReader, error)
// body: Inc "download_reader_requests"; t,err := torrentArchive.CreateTorrent;
// map storage.ErrNotFound→ErrTorrentNotFound else wrap; result:=make(chan
// streamResult,1); if !eventLoop.send(streamTorrentEvent{ns,t,result}) return
// ErrSchedulerStopped; res:=<-result; if res.torrent==nil return nil,<-res.errc;
// return newStreamReader(res.torrent, res.errc, s.clock, streamPollInterval,
// res.priority, res.request).
```

Generated mocks (one per file; produced by `make mocks`, do not hand-write):
```go
func (m *MockScheduler) DownloadReader(arg0 string, arg1 core.Digest) (scheduler.BlobReader, error)
func (mr *MockSchedulerMockRecorder) DownloadReader(arg0, arg1 interface{}) *gomock.Call
func (m *MockReloadableScheduler) DownloadReader(arg0 string, arg1 core.Digest) (scheduler.BlobReader, error)
func (mr *MockReloadableSchedulerMockRecorder) DownloadReader(arg0, arg1 interface{}) *gomock.Call
```
**Mocks must land in this PR** — adding the interface method breaks every
`MockScheduler` consumer until the mocks satisfy it (CI compile gate).
**Tests:** end-to-end on the existing scheduler harness —
`...ServesBlobWhileDownloading`, `...RandomAccessReadAt`, `...TorrentNotFound`,
`...SchedulerStopped`.
**LOC (non-test):** ~33 app code (+~33 generated mocks).

---

#### A7 — lazy activation

**Files:** `scheduler/stream_reader.go` (modified). **Imports added:** none.

```go
// streamReadahead is how many pieces past the blocked one are demanded together.
const streamReadahead = 8

// demand asks the dispatcher (lazy) to fetch [lo, hi), clamped. No-op if request nil.
func (r *streamReader) demand(lo, hi int)
// body: return if request nil; clamp lo>=0, hi<=NumPieces(); return if lo>=hi;
// build []int{lo..hi-1}; r.request(pieces).
```

`acquirePiece` DELTA — on a miss, hint + demand **once** per blocked piece via the
`hinted` guard:
```go
	// inserted before the existing waitPiece() in the miss branch:
	if r.hinted != piece {
		if r.priority != nil {
			r.priority(piece)
		}
		r.demand(piece, piece+streamReadahead)
		r.hinted = piece
	}
```
**Pairs with:** the `ctrl.dispatcher.SetLazy()` line added to
`streamTorrentEvent.apply` (deferred from A4). **Must ship together** — `SetLazy`
without the reader's `demand` deadlocks (nothing demands).
**Tests:** `TestStreamReaderReadaheadBounded` — only piece 0 present; assert the
first demanded window is exactly `[1..8]` and the readahead clamp holds; terminal
error propagates; demand is idempotent under the `hinted` guard.
**LOC (non-test):** ~33.

---

#### A8 — `ReadAt` (range)

**Files:** `scheduler/stream_reader.go` (modified). **Imports added:** none.

```go
// ReadAt reads len(p) bytes at off, spanning pieces and blocking on each as it
// streams in. Does not touch the Read/Seek cursor. Implements io.ReaderAt.
func (r *streamReader) ReadAt(p []byte, off int64) (int, error)
// body: err on off<0; up-front demand the covering span
// [off/pieceLen, (end-1)/pieceLen+1) where end=min(off+len(p), length); loop
// read<len(p): EOF if pos>=length; acquirePiece(piece); io.CopyN(io.Discard) the
// intra-piece offset; io.ReadFull the clamped want into p; close pr; advance.

var _ io.ReaderAt = (*streamReader)(nil)
```
**Reuse:** `r.demand` (A7), `r.acquirePiece` (A7), `t.PieceLength`; `closers.Close`
per piece (ReadAt closes its own readers, independent of `r.pr`).
**Tests:** `TestStreamReaderReadAtDemandsSpan` (table over off/len: mid-piece
start spanning pieces asserts demanded span + bytes; aligned full read; tail
crosses final piece → truncated `n` + `io.EOF`; `off<0` errors; read past length →
`(0, io.EOF)`); reuse the schedule-release fake to assert ReadAt blocks then
succeeds on a late piece. **LOC (non-test):** ~43.

---

#### A9 — registry read path streams

**Files:** `lib/dockerregistry/transfer/ro_transferer.go`; `ro_transferer_test.go`.
**Imports added:** `utils/closers`. **Interface:** `ImageTransferer` UNCHANGED.

```go
// Stat returns blob info; on a cache miss returns the size from torrent metainfo
// (via DownloadReader) without downloading the whole blob.
func (t *ReadOnlyTransferer) Stat(namespace string, d core.Digest) (*core.BlobInfo, error)
// body: GetFileStat(d.Hex()); on os.IsNotExist||InDownloadError → r,err :=
// sched.DownloadReader(...); defer closers.Close(r); return core.NewBlobInfo(r.Size());
// cache hit returns NewBlobInfo(fi.Size()).

// Download returns a reader; on a cache miss returns the streaming reader (serves
// bytes as pieces arrive) instead of blocking on the whole blob.
func (t *ReadOnlyTransferer) Download(
	namespace string, d core.Digest) (store.FileReader, error)
// body: GetFileReader(d.Hex()); on os.IsNotExist||InDownloadError → return
// sched.DownloadReader(...) directly (no adapter); cache hit returns the cached reader.
```

**store.FileReader conformance:** **YES, no adapter.** `store.FileReader` =
`io.Reader+io.ReaderAt+io.Seeker+io.Closer+Size() int64`; `BlobReader` =
`io.ReadSeekCloser+io.ReaderAt+Size() int64` — identical method sets, and
`*streamReader` implements all five.
**Behavior:** flips `Stat`/`Download` from blocking to streaming — Stat no longer
guarantees the blob is on disk on return. **Requires e2e coverage** (unit mocks
can't fake real piece-arrival ordering): the `estargz` harness must
assert correct `Content-Length` on a cold blob and correct bytes on mid-stream
ranged GETs.
**Tests:** `...DownloadStreamsOnCacheMiss`, `...ReadsFromCache`,
`...EmitsMBServed` (table over blob size), `...StatStreamsOnCacheMiss`,
`...MultipleDownloadsOfSameBlob` (10 concurrent, `.Times(10)`); helper
`fakeBlobReader{*bytes.Reader}`. **LOC (non-test):** ~16.

---

#### A10 — PoC cleanup

**Files:** `agentserver/server.go`; `dispatcher.go`; devcluster config;
`tagclient/client.go`. Deletions only — **no policy or config change ships
here.** The blocked piece already gets priority via `SetPriorityPiece`
(A1/A3) ahead of whatever the underlying selection policy is —
`rarest_first` stays the production default throughout this stack.
**Removal checklist** — see §6 for the full table. Drop: agentserver `?stream=1`
branch + `streamBlob` (~42 LOC, the exact +42 this branch added) and its
now-unused imports; devcluster `network_event` enablement (keep the package); the
dispatcher teardown demand log line. Keep: `mb_served` (pre-PoC metric),
devcluster `--mutex-profile-fraction` flags. Measure-then-decide:
`tagclient` `SendTimeout` 10s→30s — revert unless p99 under streaming load
justifies it.
**Tests:** existing suites must still pass after deletions; the e2e harnesses
(A9) gate rollout — no config policy flip to gate, since none ships here.
**LOC (non-test):** net ~−45 (deletions).

### 5.4 What Stack A deliberately does NOT change

- **Origin** stays a whole-blob seeder. On a cold first range request it
  materializes the full blob async (existing 202 path); the streaming benefit is
  agent↔peer P2P + warm origins. Cold-origin streaming is Stack B.
- **Tracker** stays piece-agnostic. Agents exchange bitfields directly in the
  dispatch handshake, so streaming + P2P work without it (design doc §7).
- **Backends** stay whole-object. Range fetch from cold storage is Stack B.

## 6. Pre-merge cleanup of PoC instrumentation

The PoC carried devcluster-only code that must **not** land in Stack A:

| PoC item | Action |
|---|---|
| `agent/agentserver/server.go` `?stream=1` + `streamBlob` (42 LOC) | **Drop.** The production path is the registry read path (A9); the snapshotter never hits this raw endpoint. Keep only if a non-registry streaming endpoint is independently justified. |
| `networkevent` wiring in devcluster config | **Drop** from production config. Keep the package (used by the visualization tool). Production streaming metrics use `tally.Scope` + `observability.EmitDownloadPerformance`. |
| `lazy_pieces_requested`, `mb_served`, `download_reader_requests` ad-hoc counters | Keep only those that map to real dashboards; drop the rest. ~3 LOC total. |
| dispatcher teardown `demandCount` log line | **Drop** (devcluster debugging). |
| `--mutex-profile-fraction` devcluster flags | Devcluster-only; not part of the stack. |
| `tagclient` `Get` timeout 10s→30s (`client.go`) | **Revisit, do not blindly ship.** Measure tag-lookup latency under real build-index load; only bump if justified (design doc Next). |
| `Makefile` bench targets | Optional final "bench tooling" PR; not core. |

## 7. Stack B — cold-origin range streaming (design doc §4, §5 Phase 2)

Lets a **cold** origin (blob not in its local cache) seed pieces by lazily
range-fetching them from the backend, instead of materializing the whole blob on
the first request. Today a cold origin forces a full backend download
(`blobRefresher.Refresh`, whole blob) before any byte is served — the exact stall
Stack A removes on the agent side. Stack B removes it on the origin side.

**The load-bearing constraint — integrity.** Agents CRC32-verify every received
piece against `metaInfo.GetPieceSum(pi)` (`agentstorage/torrent.go`), and the
infohash is *derived* from the piece sums (20-byte SHA1 over the bencoded `info`,
which contains `PieceSums` — `core/infohash.go:22`, `core/metainfo.go:32,37-43`).
So a cold origin must serve the **real** metainfo (cold infohash == warm
infohash) and therefore must obtain the real piece sums *without reading the
whole blob*. The mechanism that makes the whole stack possible is a **metainfo
sidecar**: at writeback the origin uploads the serialized `core.MetaInfo` as a
tiny `<digest>/kmeta` object next to the blob (~4 B/piece); a cold origin fetches
that sidecar cheaply, then range-fetches each requested piece and CRC-verifies it
normally. Integrity is preserved end-to-end; §7.3 records why no other source of
truth was used.

**As-built model (simpler than a real-bitfield leecher).** The cold-origin
partial torrent reports itself **complete** — `Complete()=true`, `HasPiece=true`,
`Bitfield()` is the full complement — and lazily range-fetches each piece inside
`GetPieceReader → ensurePiece → fetchPiece`. This is safe because origin
announces are disabled (`constructors.go`): the origin never advertises into a
swarm, it only answers piece reads on demand, so "I have everything" just means
"ask me for any piece and I will fetch it." There is **no** partial bitfield and
**no** `blobrefresh.RefreshRange` — the range fetch lives directly in
`Torrent.fetchPiece`, driven by an injected `backend.RangeDownloader`.

**The two cold seams (both served from the sidecar):**
1. HTTP metainfo (agent → origin): `origin/blobserver/server.go getMetaInfo` —
   cache miss now tries `coldMetaInfoFromSidecar` and returns `mi.Serialize()`
   (200) instead of `startRemoteBlobDownload` (202, whole blob).
2. P2P pieces + scheduler metainfo: `originstorage/torrent_archive.go
   loadMetaInfo` — cache miss → `coldMetaInfo` (sidecar + `RangeDownloader`) →
   `NewPartialTorrent`; piece reads then range-fetch on demand.

The warm path (origin already has the blob) stays byte-for-byte unchanged: a
cached blob still yields the whole-blob `NewTorrent(cas, mi)`.

**Grounding notes (verified against this branch + production GCS):**
- B1 implements the `RangeDownloader` capability for the two **real** backends
  this repo ships: **GCS** and **S3**. Both are cheap: GCS's plain SDK already
  exposes `Object.NewRangeReader(ctx, offset, length)`, and S3's
  `s3manager.Downloader` already accepts a `Range` header on `GetObjectInput` —
  neither needs new SDK surface, just a new `Client.DownloadRange` method
  layered on the existing downloader. `testfs` (devcluster-only fixture) and
  any backend lacking the capability (e.g. `hdfsbackend`) fall back to the
  unchanged whole-blob path (graceful degradation, never a regression).
- **GCS `RangeDownloader` — validated end-to-end.** Uber's internal GCS backend
  (a separate, PSC-aware implementation building on the newer
  `transfermanager.Downloader`) was implemented and unit-tested against
  production GCS as a proof that range reads work cleanly on top of Uber's real
  GCS setup: `transfermanager.DownloadObjectInput` has a `Range *DownloadRange`
  field (`{Offset, Length int64}`, `Length<0` ⇒ to-EOF) that runs through the
  same worker pool, CRC verification, and single/multi-shard logic as
  whole-object `Download` (sharding only triggers past `DownloadPartSize`, so a
  normal Kraken piece is always a single shard) — full bazel build + unit test
  pass. This repo's own `gcsbackend` (§B1) is simpler still: the plain
  `cloud.google.com/go/storage` SDK's `NewRangeReader` needs no worker pool at
  all for a single-piece range.
- Origin previously used only `*store.CAStore` (cache-only). Stack B adds a
  `*store.CADownloadStore` (separate `cache-partial` + `download` dirs) so cold
  pieces land in a sparse download file with per-piece `_status` metadata —
  mirroring the agent partial store.
- The `RangeDownloader` signature is `DownloadRange(namespace, name string, dst
  io.Writer, offset, length int64) error` (dst **before** offset/length).
  `AsRangeDownloader` unwraps `*ThrottledClient`, then type-asserts.

### 7.1 PR budget table

| PR | Scope | Files | ~LOC (non-test) | Activates? |
|----|-------|-------|------|-----------|
| B1 | `RangeDownloader` capability + GCS/S3 impls | `lib/backend/rangedownloader.go` (new) + `gcsbackend/{gcs,client}.go` + `s3backend/client.go` | ~55 | inert until B3/B4 type-assert it |
| B2 | metainfo sidecar (write at writeback) | `lib/metainfosidecar/sidecar.go` (new) + `persistedretry/writeback/executor.go` | ~55 | `/kmeta` sidecar lands on backend |
| B3 | origin partial torrent (lazy range-fetch) | `originstorage/pieces.go` (new) + `originstorage/torrent.go` | ~150 (2 PRs) | partial `Torrent` fetches on demand |
| B4 | cold-origin wiring (both seams) | `originstorage/torrent_archive.go`, `scheduler/constructors.go`, `origin/cmd/{cmd,config}.go`, `config/origin/base.yaml`, `origin/blobserver/server.go` | ~100 | **cold origin seeds partial content** |

**Dependency order:** B1 and B2 are independent (different packages) and can land
in parallel. **B3 depends on B1** (the partial `Torrent` holds a
`backend.RangeDownloader`). **B4 depends on B1+B2+B3** — it fetches the B2 sidecar
through B1's `AsRangeDownloader` and constructs B3's `NewPartialTorrent`. B3 is
the long pole; split it 2 ways: **B3a** lifts the `pieces.go` per-piece state
model + the `NewPartialTorrent` constructor (reports complete); **B3b** adds the
lazy fetch state machine (`GetPieceReader`/`ensurePiece`/`waitForPiece`/
`fetchPiece`/`markPieceComplete`).

### 7.2 PR detail

#### B1 — `RangeDownloader` backend capability + GCS/S3 implementations

**Files:** `lib/backend/rangedownloader.go` (new — capability iface + unwrap
helper); `lib/backend/gcsbackend/{gcs,client}.go` (impl `DownloadRange`);
`lib/backend/s3backend/client.go` (impl `DownloadRange`). Callers type-assert,
so every backend lacking the capability (`hdfsbackend`, etc.) keeps working via
the whole-blob fallback — no interface break.
**Imports added:** `rangedownloader.go`: `io`. `gcsbackend`: none new (`storage`
already imported). `s3backend`: none new (`aws`, `s3`, `s3manager` already
imported).
**Declarations:**
```go
// lib/backend/rangedownloader.go — optional capability, sibling to Client.
// Callers MUST type-assert; backends lacking it fall back to whole-blob Download.
type RangeDownloader interface {
    DownloadRange(namespace, name string, dst io.Writer, offset, length int64) error
}

// AsRangeDownloader unwraps *ThrottledClient (which embeds but does not forward
// the method) and type-asserts. ok=false ⇒ caller falls back to whole-blob.
func AsRangeDownloader(c Client) (RangeDownloader, bool)
// body: if tc, ok := c.(*ThrottledClient); ok { c = tc.Client }; rd, ok :=
//   c.(RangeDownloader); return rd, ok.

// lib/backend/gcsbackend/gcs.go — extend the GCS interface.
type GCS interface {
    // ...existing methods unchanged...
    DownloadRange(objectName string, w io.Writer, offset, length int64) (int64, error)
}

// lib/backend/gcsbackend/client.go — GCSImpl.DownloadRange reuses the plain
// storage SDK's native range read; no new SDK surface needed.
func (g *GCSImpl) DownloadRange(
    objectName string, w io.Writer, offset, length int64) (int64, error)
// body: rc, err := g.bucket.Object(objectName).NewRangeReader(g.ctx, offset,
//   length); isObjectNotFound(err) ⇒ backenderrors.ErrBlobNotFound; defer
//   closers.Close(rc); return io.Copy(w, rc).

// Client.DownloadRange satisfies backend.RangeDownloader on top of GCSImpl.
func (c *Client) DownloadRange(
    namespace, name string, dst io.Writer, offset, length int64) error
// body: path := c.pather.BlobPath(name); _, err := c.gcs.DownloadRange(path,
//   dst, offset, length); return err.

// lib/backend/s3backend/client.go — reuses the existing s3manager.Downloader;
// GetObjectInput.Range is the only new field, no new SDK call shape.
func (c *Client) DownloadRange(
    namespace, name string, dst io.Writer, offset, length int64) error
// body: path := c.pather.BlobPath(name); writerAt fallback identical to
//   Download (io.WriterAt or rwutil.CappedBuffer); input := &s3.GetObjectInput{
//   Bucket, Key: path, Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset,
//   offset+length-1))}; c.s3.Download(writerAt, input); isNotFound(err) ⇒
//   backenderrors.ErrBlobNotFound; drain CappedBuffer into dst as Download does.
```
**Call-site edits:** none outside the two backend packages — `RangeDownloader`
is a capability callers discover via `AsRangeDownloader`, not a method added to
the `backend.Client` interface itself.
**Tests:** `gcsbackend/client_test.go` `TestClientDownloadRange` and
`s3backend/client_test.go` `TestClientDownloadRange` — both table-driven against
their existing mock SDK fakes: first range, interior range, length-past-EOF
clamp, not-found → `backenderrors.ErrBlobNotFound`.
**LOC (non-test):** ~55 (`rangedownloader.go` ~12, gcsbackend ~20, s3backend ~23).
**Backends without this PR** (`hdfsbackend`, `testfs`, etc.) simply don't
implement `RangeDownloader`; `AsRangeDownloader` returns `(nil, false)` and
callers fall back to the unchanged whole-blob `Download` path — never a
regression. `testfs` (the devcluster-only fixture backend) is intentionally
out of scope for this production PR; if a devcluster range-streaming demo is
wanted later, that's a separate, non-production change.

#### B2 — metainfo sidecar (shared helper + writeback write)

**Files:** `lib/metainfosidecar/sidecar.go` (new — shared so writeback and
originstorage don't depend on each other); `lib/persistedretry/writeback/executor.go`
(write the sidecar after the blob upload).
**Imports added:** `sidecar.go`: `bytes`, `core`, `lib/backend`. `executor.go`:
`bytes`, `lib/metainfosidecar`.
**Declarations:**
```go
// lib/metainfosidecar/sidecar.go
const Suffix = "/kmeta"
func Name(name string) string { return name + Suffix }

func Fetch(c backend.Client, namespace string, d core.Digest) (*core.MetaInfo, error)
// body: var buf bytes.Buffer; c.Download(namespace, Name(d.Hex()), &buf);
//   core.DeserializeMetaInfo(buf.Bytes()). Sidecar is tiny (~4 B/piece), so a
//   plain whole-object Download is used — no range needed.

// executor.go — FileStore gains read access to local metainfo.
type FileStore interface {
    DeleteCacheFileMetadata(name string, md metadata.Metadata) error
    GetCacheFileReader(name string) (store.FileReader, error)
    GetCacheFileMetadata(name string, md metadata.Metadata) error // NEW
}

func (e *Executor) uploadMetaInfoSidecar(
    ctx context.Context, client backend.Client, t *Task) error
// body: idempotent — client.Stat(ns, metainfosidecar.Name(t.Name)) == nil ⇒
//   return nil; var tm metadata.TorrentMeta;
//   e.fs.GetCacheFileMetadata(t.Name, &tm) (os.IsNotExist ⇒ skip, no local
//   metainfo); b, _ := tm.Serialize();
//   client.Upload(ns, metainfosidecar.Name(t.Name), bytes.NewReader(b)).
```
**Call-site edits:** `executor.upload` — replace the `client.Stat`-exists early
return with a `blobExists bool`; wrap the blob `GetCacheFileReader` + `Upload` in
`if !blobExists`; then **always** call `uploadMetaInfoSidecar` (so a re-push of an
already-present blob still backfills the sidecar). `*store.CAStore` already
implements `GetCacheFileMetadata`, so the existing `cmd.go` wiring still satisfies
the widened `FileStore`.
**Tests:** `executor_test.go` (extend) — after `Exec`, the backend holds both
`name` and `name+"/kmeta"`, and the sidecar deserializes to the local
`TorrentMeta`; include the blob-already-exists case (sidecar still written).
**LOC (non-test):** ~55 (`sidecar.go` ~14, executor `blobExists` refactor ~10,
`uploadMetaInfoSidecar` ~30, iface +1).

#### B3 — origin partial torrent (lazy range-fetch) — splits B3a/B3b

**Files:** `lib/torrent/storage/originstorage/pieces.go` (new — per-piece status
model, adapted from `agentstorage/pieces.go`);
`lib/torrent/storage/originstorage/torrent.go` (partial mode alongside the
unchanged warm `NewTorrent`).
**Imports added:** `torrent.go`: `io`, `time`, `utils/closers`, `atomic`
(`bitset` present); `pieces.go`: `sync`, `lib/store/metadata`.
**Declarations (B3a — state model + constructor):**
```go
// pieces.go — per-piece status persisted as one byte each.
const _pieceStatusSuffix = "_status"
type pieceStatus int
const ( _empty pieceStatus = iota; _complete; _dirty ) // _dirty in-memory only
type pieceStatusMetadata struct{ statuses []pieceStatus } // Serialize/Deserialize
type piece struct { sync.RWMutex; status pieceStatus }
func (p *piece) snapshot() pieceStatus
func (p *piece) complete() bool
func (p *piece) tryMarkDirty() (dirty, complete bool) // claims the fetch
func (p *piece) markEmpty()
func (p *piece) markComplete()
func restorePieces(
    d core.Digest, cads *store.CADownloadStore, numPieces int) ([]*piece, int, error)
// body: GetOrSetMetadata(_status) seeded empty; rebuild []*piece + completed
//   count; tolerate cads.InCacheError (already moved to cache).

// torrent.go — partial fields appended to the warm Torrent (nil/false in warm).
const (
    _partialFetchPollInterval = 50 * time.Millisecond
    _partialFetchTimeout      = 2 * time.Minute
)
type Torrent struct {
    metaInfo    *core.MetaInfo
    cas         *store.CAStore
    numComplete *atomic.Int32
    partial     bool                    // NEW
    cads        *store.CADownloadStore  // NEW
    rd          backend.RangeDownloader // NEW
    namespace   string                  // NEW
    pieces      []*piece                // NEW
}
func NewPartialTorrent(
    cads *store.CADownloadStore, rd backend.RangeDownloader,
    namespace string, mi *core.MetaInfo) (*Torrent, error)
// body: cads.CreateDownloadFile(mi.Digest().Hex(), mi.Length()) tolerating
//   InDownloadError/InCacheError; restorePieces(...); numComplete =
//   mi.NumPieces() (reports complete). Warm NewTorrent(cas, mi) unchanged.
```
B3a keeps the reported-complete invariant: `Complete()=true`, `HasPiece=true`,
`Bitfield()=Complement()`, `MissingPieces()=[]` — unchanged from today, so the
torrent advertises every piece and the dispatcher asks for any of them on demand.
**Declarations (B3b — lazy fetch state machine):**
```go
func (t *Torrent) GetPieceReader(pi int) (storage.PieceReader, error)
// body: partial ⇒ ensurePiece(pi) then NewFileReader(getFileOffset, PieceLength,
//   &downloadOpener{t}); warm ⇒ NewFileReader(..., &opener{t}) (unchanged).
func (t *Torrent) ensurePiece(pi int) error
// body: fast-path p.complete(); tryMarkDirty(): complete⇒nil, dirty⇒
//   waitForPiece(p), else elected fetcher ⇒ fetchPiece (on err markEmpty) ⇒
//   markPieceComplete.
func (t *Torrent) waitForPiece(p *piece) error
// body: spin-poll p.snapshot() every _partialFetchPollInterval; _complete⇒nil,
//   _empty⇒err, deadline _partialFetchTimeout⇒err.
func (t *Torrent) fetchPiece(pi int) error
// body: f := cads.GetDownloadFileReadWriter(digest); f.Seek(getFileOffset(pi));
//   h := core.PieceHash(); rd.DownloadRange(namespace, digest,
//   io.MultiWriter(f, h), getFileOffset(pi), PieceLength(pi)); if h.Sum32() !=
//   metaInfo.GetPieceSum(pi) ⇒ errors.New("invalid piece sum").
func (t *Torrent) markPieceComplete(pi int) error
// body: cads.Download().SetMetadataAt(digest, &pieceStatusMetadata{},
//   []byte{byte(_complete)}, int64(pi)); pieces[pi].markComplete().
type downloadOpener struct{ torrent *Torrent }
func (o *downloadOpener) Open() (store.FileReader, error)
// body: cads.Any().GetFileReader(digest).
```
**Call-site edits:** none outside `originstorage` until B4 — the warm
`NewTorrent(cas, mi)` signature is unchanged, so existing construction still
compiles.
**Tests:** `torrent_test.go` (extend) against a real `*store.CADownloadStore`
fixture + a fake in-memory `RangeDownloader`: (a) first `GetPieceReader(pi)` does
exactly one `DownloadRange`, correct bytes; second call zero further fetches;
(b) sum mismatch ⇒ error + re-fetchable; (c) N concurrent goroutines on one piece
⇒ exactly one `DownloadRange`; (d) restart durability via a second Torrent over
the same `cads`; (e) short last piece length.
**LOC (non-test):** ~150 across 2 PRs (`pieces.go` ~80 [B3a]; `torrent.go`
constructor + state machine ~70 [B3a ~25 / B3b ~45]).

#### B4 — cold-origin wiring (both seams)

**Files:** `originstorage/torrent_archive.go` (cold metainfo + partial torrent
selection); `lib/torrent/scheduler/constructors.go` (`NewOriginScheduler`
params); `origin/cmd/config.go` + `origin/cmd/cmd.go` (construct the
`CADownloadStore`, pass it + `backendManager`); `config/origin/base.yaml`
(`cadownloadstore:` block); `origin/blobserver/server.go` (HTTP metainfo cold
branch).
**Imports added:** `torrent_archive.go`: `lib/metainfosidecar`; `server.go`:
`lib/metainfosidecar` (`backend` present).
**Declarations:**
```go
// torrent_archive.go — archive now holds cads + backends.
type TorrentArchive struct {
    cas           *store.CAStore
    cads          *store.CADownloadStore
    backends      *backend.Manager
    blobRefresher *blobrefresh.Refresher
}
func (a *TorrentArchive) loadMetaInfo(
    namespace string, d core.Digest) (*core.MetaInfo, backend.RangeDownloader, error)
// body: warm cache GetCacheFileMetadata ⇒ (tm.MetaInfo, nil, nil); else if
//   os.IsNotExist ⇒ coldMetaInfo ⇒ (mi, rd, nil); else blobRefresher.Refresh +
//   return errors.New("refreshing blob") (today's behavior).
func (a *TorrentArchive) coldMetaInfo(
    namespace string, d core.Digest) (*core.MetaInfo, backend.RangeDownloader, bool)
// body: backends.GetClient(namespace); backend.AsRangeDownloader (false⇒bail);
//   metainfosidecar.Fetch (err⇒debug log, false).
// GetTorrent: rd != nil ⇒ NewPartialTorrent(cads, rd, namespace, mi);
//   else NewTorrent(cas, mi). Stat: loadMetaInfo (ignore rd) + complement bitfield.

// origin/blobserver/server.go — getMetaInfo cold branch.
func (s *Server) coldMetaInfoFromSidecar(
    namespace string, d core.Digest) (*core.MetaInfo, bool)
// body: same shape as coldMetaInfo: GetClient → AsRangeDownloader (capability
//   gate) → metainfosidecar.Fetch. getMetaInfo, on os.IsNotExist, tries this and
//   returns mi.Serialize() (200) before falling back to startRemoteBlobDownload.
```
**Call-site edits:**
- `scheduler/constructors.go NewOriginScheduler` gains `cads *store.CADownloadStore`
  + `backends *backend.Manager`, passing both to
  `originstorage.NewTorrentArchive(cas, cads, backends, blobRefresher)`.
- `origin/cmd/config.go`: `Config` gains
  `CADownloadStore store.CADownloadStoreConfig \`yaml:"cadownloadstore"\``.
- `origin/cmd/cmd.go`: `cads, err := store.NewCADownloadStore(config.CADownloadStore,
  stats)`; pass `cads` + `backendManager` into `NewOriginScheduler`.
- `config/origin/base.yaml`: add a `cadownloadstore:` block (separate
  `cache-partial` + `download` dirs so it never collides with `castore.cache_dir`).
**Tests:** `torrent_archive_test.go` (extend) — cold digest with a sidecar on a
fake in-memory `backend.Client`+`RangeDownloader` fixture ⇒ `GetTorrent`
returns a partial torrent and `Stat` the complement bitfield; no sidecar /
non-range backend (a fake `backend.Client` that does not implement
`RangeDownloader`) ⇒ falls back to `blobRefresher.Refresh` (error), proving
graceful degradation.
**LOC (non-test):** ~100 (`torrent_archive.go` ~55, `blobserver` ~22,
`constructors`/`cmd`/`config`/yaml ~23).

### 7.3 Alternatives considered for cold-origin metainfo

The sidecar is load-bearing, so we evaluated three other ways to give a cold
origin the real piece sums (and thus the real infohash) without reading the whole
blob. All three were rejected; the findings are recorded so the choice isn't
relitigated.

**(1) Centralized metainfo store (redis / tracker / build-index / SQL).** Keep
generation where it is (`metainfogen` needs the full blob and already runs at
writeback) but publish the metainfo to a shared service instead of a per-blob
sidecar.
- *Pros:* a queryable fleet-wide index; no backend `List`/GC pollution with
  `/kmeta` objects.
- *Cons (decisive):* every candidate host adds a **new failure domain on the hot
  cold-pull path**. The sidecar co-locates with the very backend the origin must
  already reach to fetch pieces, so it introduces **zero** new failure modes; a
  redis/tracker/build-index dependency introduces one. redis is the lightest
  (~250–350 LOC) but gives the origin a dependency it doesn't have today; the
  tracker is circular (the origin is the tracker's source of truth, and it would
  stop being stateless); build-index is the wrong granularity (tag→digest, not
  digest→metainfo); a "SQL store" is really a sidecar-in-a-database with extra
  ops. All need a **separate GC keyed to blob existence** and put metainfo state
  origin-side. The sidecar is ~80 LOC and self-cleaning (lives and dies with the
  blob, alongside it on the same backend).
- *Verdict:* sidecar wins unless a future need for fleet-wide metainfo
  querying/prefetch justifies the index — not in scope.

**(2) Change the integrity model so the infohash no longer needs piece sums.**
The infohash is a 20-byte SHA1 over the bencoded `info` that *contains*
`PieceSums`, and it is the swarm key end-to-end (`peerstore` keys on
`map[core.InfoHash]` at `tracker/peerstore/local.go:42`; conn dispatch
`s.torrentControls[...InfoHash()]` at `lib/torrent/scheduler/state.go:102` and
`events.go:160`).
- *Variant A — infohash = content digest.* A flag-day break: it partitions every
  in-flight swarm (old vs new key), ripples a 20-byte SHA1 vs the 32-byte digest
  type through peerstore/announce/dispatch, and loses piece-length
  disambiguation. ~400–800+ LOC. Not recommended.
- *Variant B — defer integrity to a whole-blob SHA256 and drop per-piece sums.*
  Security is acceptable in a non-adversarial datacenter, and `CAStore.verify`
  already exists (`lib/store/ca_store.go:335`). But to actually help a cold origin
  you must remove `PieceSums` from `info` — the same swarm-key split as Variant A
  — so it doesn't solve the problem. (A whole-blob SHA256 *at completion* is a
  worthwhile orthogonal defense-in-depth, not a sidecar substitute.)
- *Verdict:* the sidecar is the only option that preserves the swarm key and
  rolls out incrementally (cold and warm origins interoperate from day one).

**(3) Storage-layer sources (object metadata / native checksums / lazy sidecar).**
Metainfo is small (~11 B/piece + ~120 B JSON: 100 MB→~0.4 KB, 1 GB→~2.9 KB,
20 GB→~55 KB).
- *Object user-metadata:* S3 caps user-metadata at 2 KB (fails at ~700 MB blobs)
  and GCS at ~8 KB; covering 20 GB would force a global piece-length change. Not
  viable for Kraken's 20 GB target.
- *Native backend checksums (S3 part ETags / GCS CRC32C):* don't supply the
  infohash (still derived from Kraken's piece sums) and mismatch on both chunk
  boundaries and algorithm (CRC32C vs Kraken's CRC32-IEEE). Partial at best.
- *Lazy ranged sidecar:* the infohash needs **all** piece sums up front (`Stat`
  builds the full bitfield), so a partially-fetched sidecar defeats itself; and
  the sidecar is already KB-scale, so there is nothing to save.
- *Verdict:* a separate, whole-object `/kmeta` sidecar is the right call.

## 8. Stack C — pluggable index-replication seam (deferred, format-agnostic)

**Scope change:** v1 supports **stargz only**. stargz's TOC is embedded in each
converted layer (no separate index artifact), so it needs **no** `lib/streaming`
entry at all — single-cluster and cross-cluster replication both already work
via the normal manifest/layer replication path. Stack C is therefore **not
needed for v1** and is not being built now.

It is kept here only as a **format-agnostic extension point**, in case a future
lazy format is adopted that (unlike stargz) ships a separate index artifact
needing its own cross-cluster dependency resolution. If that need arises, add:

- A minimal `lib/streaming` seam: an `IndexFormat` interface
  (`Name() string`, `DependencyDigests(index io.Reader) (core.DigestList,
  error)`) plus a small name→implementation `Registry`, mirroring the pattern
  `build-index/tagtype.Map` already uses for `DependencyResolver`.
- One `tagtype.DependencyResolver` implementation per format needing it, wired
  into `tagtype.NewMap`'s existing switch (`tagtype/map.go`), matched by
  namespace pattern ahead of the `docker` catch-all.

No format currently requires this. Do not implement `lib/streaming` or any
concrete `IndexFormat` until a real format needs it — building it against a
hypothetical future format risks locking in the wrong interface shape.


## 9. Stack D — tracker partial-aware discovery (design doc §7)

Tightens cold-start P2P: with partial peers the tracker can hand a leecher peers
that already hold the pieces it needs, instead of the current binary
seeder/non-seeder split. This is a **cold-start optimization, not a correctness
gap** — agents already exchange bitfields directly in the dispatch handshake, so
streaming + P2P work without it; D1/D2 are inert until both ship and a
coverage-aware policy is configured.

**Grounding notes (verified against master):**
- Announce **is** already versioned: `V1=1`, `V2=2` consts in
  `tracker/announceclient/client.go`; the route table registers per-version paths
  and the announcer calls with `V2`. V3 follows the existing scheme (it is **not**
  introducing versioning). (Design §7 cites
  `lib/torrent/scheduler/announceclient/`; the real path is
  `tracker/announceclient/`.)
- `core.PeerInfo` currently carries only `Complete bool` — a bitfield is genuinely
  new state. `peerstore` serializes peers as `pid:ip:port:complete` (redis) and a
  `peerEntry` struct (local); both must gain a bitfield field.
- Handout selection is `getPeerHandout` → `peerStore.GetPeers` +
  `originStore.GetOrigins` → `policy.SortPeers` (`PriorityPolicy`, with
  `assignmentPolicy.assignPriority(peer)`). `assignPriority` takes only a peer
  today, so D2 must thread the requested pieces through its signature.

### 9.1 PR budget table

| PR | Scope | Files | ~LOC (non-test) | Activates? |
|----|-------|-------|------|-----------|
| D1 | V3 announce carrying progress/bitfield | `core/peer_info.go` + `announceclient` + `trackerserver` + `peerstore` | ~95 | inert until D2 ranks on it |
| D2 | coverage-aware handout policy | `tracker/peerhandoutpolicy/*` + `trackerserver/announce.go` | ~70 | **tracker prefers covering peers (opt-in)** |

**Dependency order:** D1 → D2 (D2 ranks on the bitfield D1 stores). D1 is
back-compatible (V1/V2 announces leave the bitfield nil); D2 ships a **new** named
policy so the existing `completeness`/`default` ordering is unchanged unless
configured.

### 9.2 PR detail

#### D1 — V3 announce carrying progress/bitfield

**Files:** `core/peer_info.go` (progress fields + ctor); `announceclient/client.go`
(`V3` const, extend `Request`, version-aware marshal, extend `Announce`);
`trackerserver/announce.go` (`announceHandlerV3`); `trackerserver/server.go` (V3
route); `lib/torrent/scheduler/announcer/announcer.go` (pass bitfield + `V3`);
`tracker/peerstore/{store,redis,local}.go` (persist bitfield);
`mocks/tracker/announceclient/client.go` (regenerated).
**Imports added:** `core/peer_info.go`: none if bitfield is `[]bool` (keeps `core`
free of a `bitset` dep and matches the networkevent wire shape). **Required
before shipping to any real cluster** (per `lazy-pull-streaming-critical-
review.md`, which warns against serializing large `[]bool` arrays in
production): use a packed bitfield (`[]byte`) or compact ranges instead of raw
`[]bool`, and cap the announce payload size — this is a decision to make in
this PR, not a follow-up. `announce.go`/`announcer.go`: none.
**Declarations:**
```go
// core/peer_info.go — extend PeerInfo (after Complete bool):
type PeerInfo struct {
    PeerID   PeerID `json:"peer_id"`
    IP       string `json:"ip"`
    Port     int    `json:"port"`
    Origin   bool   `json:"origin"`
    Complete bool   `json:"complete"`
    // Bitfield is the peer's per-piece have-set; len==numPieces, nil pre-V3.
    Bitfield []bool `json:"bitfield,omitempty"`
    // NumComplete is a cheap progress summary (== set-bit count; 0 if nil).
    NumComplete int `json:"num_complete,omitempty"`
}

// Variadic option keeps the ~12 existing 5-arg NewPeerInfo call sites compiling.
func NewPeerInfo(
    peerID PeerID, ip string, port int, origin, complete bool,
    opts ...PeerInfoOption) *PeerInfo // body: base struct + apply opts
type PeerInfoOption func(*PeerInfo)
func WithBitfield(b []bool) PeerInfoOption // body: set Bitfield + NumComplete
func PeerInfoFromContext(
    pctx PeerContext, complete bool, bitfield []bool) *PeerInfo

// tracker/announceclient/client.go
const V3 = 3
// Request needs no new top-level field — Peer *core.PeerInfo now carries the
// bitfield; V3 simply stops zeroing it. Response stays []*core.PeerInfo.
func getEndpoint(version int, addr string, h core.InfoHash) (method, url string)
// body: V1→GET /announce; V2→POST /announce/{h}; V3→POST /announce/v3/{h}.
func (c *client) Announce(
    d core.Digest, h core.InfoHash, complete bool, bitfield []bool,
    version int) ([]*core.PeerInfo, time.Duration, error)
// body: PeerInfoFromContext(c.pctx, complete, bitfield); for version<V3 nil out
// Peer.Bitfield before marshal (back-compat); else send it.

// tracker/trackerserver/announce.go — V3 handler mirrors V2; s.announce already
// passes the whole *core.PeerInfo to UpdatePeer + handout, so only req.Peer
// .Bitfield is newly populated.
func (s *Server) announceHandlerV3(w http.ResponseWriter, r *http.Request) error
```
**Call-site edits:**
- `trackerserver/server.go` `Handler()`: add
  `r.Post("/announce/v3/{infohash}", handler.Wrap(s.announceHandlerV3))` beside V2.
- `announcer/announcer.go`: call `Announce(d, h, complete, bitfield,
  announceclient.V3)` with `bitfield` from the dispatcher's `torrent.Bitfield()`
  converted to `[]bool`; origins keep `Disabled()`. Extend
  `announceclient.DisabledClient.Announce` to match (still `ErrDisabled`).
- `peerstore`: redis `serialize/deserializePeer` extend `pid:ip:port:complete`
  with a packed-bitfield 5th field (omitted-empty so legacy 4-field still
  parses); `local.peerEntry` gains `bitfield []bool`. **Verify**: redis set-member
  size grows with numPieces — store packed bytes or keep only `NumComplete` in
  redis and the full bitfield in `local`.
**Tests:** `func TestAnnounceV3CarriesBitfield` — V3 round-trips
`Bitfield`+`NumComplete`; V2 leaves `Bitfield` nil; empty/nil; len mismatch
tolerated. `func TestPeerInfoWithBitfield` — `NumComplete` == set-bit count; nil→0.
`func TestRedisStoreBitfieldRoundTrip` / `TestLocalStoreBitfieldRoundTrip` —
serialize parity, legacy 4-field still parses. `func TestGetEndpointV3`.
Regenerate `announceclient.Client` mocks (`make mocks`).
**LOC (non-test):** ~95.

#### D2 — handout policy preferring peers covering requested pieces

**Files:** `tracker/peerhandoutpolicy/peerhandoutpolicy.go` (thread requested
pieces into ranking); `peerhandoutpolicy/completeness_policy.go` (coverage-aware
variant); `peerhandoutpolicy/config.go` (register the policy name);
`trackerserver/announce.go` (`getPeerHandout` passes requested pieces);
`announceclient/client.go` (optional `RequestedPieces []int` on `Request`).
**Imports added:** none (`core`, `sort`, `tally` present).
**Declarations:**
```go
// announceclient/client.go — optional V3-only field so a lazy peer advertises
// the pieces it currently wants.
type Request struct {
    Name            string         `json:"name"`
    Digest          *core.Digest   `json:"digest"`
    InfoHash        core.InfoHash  `json:"info_hash"`
    Peer            *core.PeerInfo `json:"peer"`
    RequestedPieces []int          `json:"requested_pieces,omitempty"` // V3
}

// peerhandoutpolicy.go — assignmentPolicy gains the requesting context.
type assignmentPolicy interface {
    assignPriority(
        source, peer *core.PeerInfo, requested []int) (priority int, label string)
}
func (p *PriorityPolicy) SortPeers(
    source *core.PeerInfo, peers []*core.PeerInfo, requested []int) []*core.PeerInfo
// body: unchanged loop but call assignPriority(source, peer, requested); sort
// SliceStable by priority asc; source exclusion + emitNumSeeders unchanged.

// completeness_policy.go — coverage-aware variant (new name; keeps existing
// "completeness" behavior byte-identical and opt-in).
const _coveragePolicy = "coverage"
type coverageAssignmentPolicy struct{}
func newCoverageAssignmentPolicy() assignmentPolicy
func (p *coverageAssignmentPolicy) assignPriority(
    source, peer *core.PeerInfo, requested []int) (int, string)
// body: complete seeder→0 "peer_seeder"; origin→1 "origin"; covers ≥1 requested
// piece→2 "peer_partial_covering"; else→3 "peer_incomplete". (Verify: priority is
// the sole sort key today; to rank by *amount* covered, widen SortPeers to a
// (priority, -covered) tuple. Coarse buckets keep the int-priority contract.)

// Back-compat shims: defaultAssignmentPolicy + completenessAssignmentPolicy
// .assignPriority gain (source, requested) params and ignore them.
```
**Call-site edits:**
- `trackerserver/announce.go` `getPeerHandout`: `return s.policy.SortPeers(peer,
  peers, req.RequestedPieces), nil` — thread `requested []int` from
  `announceHandlerV3`; V1/V2 handlers pass `nil`.
- `peerhandoutpolicy/config.go` `NewPriorityPolicy` switch: add
  `case _coveragePolicy: p.policy = newCoverageAssignmentPolicy()`.
- `default_policy.go` + `completeness_policy.go`: update both `assignPriority`
  signatures to the 3-arg form (params ignored); update `fixtures.go` and all
  `SortPeers`/`assignPriority` call sites to the new arity.
**Tests:** `func TestCoveragePolicySortPeers` — complete seeder first regardless of
requested; partial peer covering requested ranked ahead of non-covering
incomplete; nil/empty requested degrades to completeness ordering (no regression);
nil peer bitfield ⇒ zero coverage; origin placement unchanged; source excluded.
`func TestCoverageAssignPriority` — covered-count buckets; out-of-range requested
index ignored. Existing completeness/default tests updated for the new arity; no
mocks (concrete `PriorityPolicy`).
**LOC (non-test):** ~70.

## 10. Deferred: Stack E — per-piece zstd (separate workstream)

**Out of scope for this plan.** Streaming + zstd are compatible **iff** zstd is
**per-piece** (each `PieceLength` chunk an independent frame). Whole-blob
single-stream zstd breaks the 1:1 offset↔piece mapping and is incompatible with
range/lazy pull. This stack must coordinate with the separate zstd effort to
ensure per-piece framing, not whole-blob — so it is tracked there, not detailed
here.

## 11. Testing strategy

- **Unit (each PR):** table-driven, `testify/require`. A1 tests priority
  reservation with fakes; A2/A3 test demand restriction + injection; A5/A7/A8 test
  the reader against a fake `storage.Torrent` that releases pieces on a schedule
  (sequential read, seek, range, lazy-demand window, EOF, terminal error).
- **Scheduler integration:** extend existing scheduler tests so `DownloadReader`
  returns before completion and serves pieces as they land (eager after A6, lazy
  after A7).
- **Stack B:** B1 tests `DownloadRange` against each real backend's existing
  mock SDK fake (`gcsbackend`, `s3backend`) — first/interior/past-EOF-clamp,
  not-found → `backenderrors.ErrBlobNotFound`; B2 tests that `Exec` writes
  both the blob and the `/kmeta` sidecar (incl. the blob-already-exists backfill)
  and that the sidecar round-trips to the local `TorrentMeta`; B3 tests the
  partial `Torrent` against a real `*store.CADownloadStore` fixture + a fake
  `RangeDownloader` (one `DownloadRange` per piece, sum-mismatch re-fetch,
  single-fetch under concurrency, restart durability); B4 tests `loadMetaInfo`
  cold (sidecar present ⇒ partial torrent) and the whole-blob fallback when a
  backend lacks `RangeDownloader` or has no sidecar.
- **Stack C:** deferred (§8) — no tests until a concrete format needs the
  index-replication seam.
- **Stack D:** D1 tests V3 announce round-trip + V2 back-compat + peerstore
  bitfield serialize parity (incl. legacy 4-field parse); D2 tests the
  coverage-aware `SortPeers`/`assignPriority` ranking and that nil/empty requested
  pieces degrade to the existing completeness ordering (no regression).
- **e2e (post-A9):** the existing `examples/devcluster/estargz`
  harness over `make devcluster` — assert time-to-running ≪ overlayfs, bytes
  fetched ≪ full image, `remote-snapshot-prepared:true > 0`, 0 fallback errors,
  and the agent↔agent P2P share > 0 (`p2p_agent_benchmark.sh`). The **cold-origin**
  variant (Stack B) is already wired into the estargz harness: `run_e2e.sh`
  `cold_origin` POSTs `forcecleanup?ttl_hr=0` (writeback first, so the blob +
  `/kmeta` are on the backend, then the warm cache is wiped) before the lazy leg,
  and `estargz_benchmark.sh` parses the testfs download log to assert the cold
  origin range-fetched only touched pieces (`/kmeta` 200 + per-piece 206, zero
  full 200 blob GETs) — far below the full image. **Note:** devcluster only
  ships `testfs` as a backend, so this harness needs `testfs.DownloadRange` as
  a local dev fixture to exercise the cold-origin path end-to-end; that's
  devcluster tooling wired separately from B1 (§7.2), not part of the
  reviewable production PR stack, and carries no production backend claim.

## 12. Open questions

1. **Priority mechanism has no per-stream classification.**
   `docs/lazy-pull-streaming-critical-review.md` verified today's priority
   mechanism (A1's `priority map[int]struct{}`, ascending-sorted) has no
   per-stream or "currently blocked piece" classification — a reader blocked
   on a high-index piece can lose pipeline slots to another stream's
   lower-index demand or stale readahead. Production needs a stream-aware
   priority overlay — blocked piece > exact `ReadAt` span > sequential
   readahead > background fill — before lazy streaming safely coexists with
   multiple concurrent streams (or other swarm traffic) on the same agent.
   This overlay is scoped as future work, not part of this stack.
2. **Stack B concurrency and observability gap (new, per critical review).**
   Confirmed absent in code: no per-origin or per-blob cap on concurrent
   `RangeDownloader.DownloadRange` fetches, and no metrics for range-206 count,
   bytes fetched, duplicate-waiter count, or fallback-to-full-refresh anywhere
   in `originstorage/torrent.go`, `torrent_archive.go`, or
   `lib/backend/rangedownloader.go`. A concurrent cold start today can fan out
   unbounded parallel range fetches per blob with zero visibility into
   fallback behavior. Required before Stack B is treated as fleet-ready; not
   required for the single-cluster devcluster PoC.
3. **Piece-length vs. snapshotter chunk alignment (resolved)** — see design doc
   Open Question #3. Read amplification is bounded by whole-piece CRC32
   verification (the load-bearing constraint). Fix: byte-budgeted readahead
   (`streamReadaheadBytes`, default 32 MiB) applied only on sequential `Read`;
   `ReadAt` passes readahead=0 (priority hint only, no overshoot). Pays twice
   (P2P + cold-origin backend egress).
4. **`tagclient` timeout** under real build-index latency (see §6).
5. **`/kmeta` sidecar lifecycle (Stack B).** The sidecar is written at writeback
   and read on cold pulls, but nothing deletes it when its blob is GC'd from the
   backend, and it is currently `/kmeta`-suffixed before `BlobPath` (valid for
   the identity pather; other pathers are out of PoC scope). Resolve: tie sidecar
   deletion to blob deletion (or a TTL sweep), and confirm the suffix survives
   each production pather. Config/policy, plus a small deletion hook.
