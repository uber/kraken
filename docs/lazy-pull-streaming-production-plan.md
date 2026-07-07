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
   built now — v1 supports stargz-family formats (eStargz, zstd:chunked)
   only, neither of which needs a separate index blob.

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

The stack lands two new production surfaces, both fully declared (with
Files/Declarations/Tests/LOC) in §5.3's A6:

- **`BlobReader`** (`lib/torrent/scheduler/scheduler.go`, new type) — serves a
  blob that may still be downloading: reads block per-piece, not per-blob,
  until the covering pieces land. Satisfies `store.FileReader` + `io.ReaderAt`
  so the registry read path's `http.ServeContent` can range it.
- **`Scheduler.DownloadReader(namespace string, d core.Digest) (BlobReader,
  error)`** (new method on the existing `Scheduler` interface) — the
  non-blocking entry point: returns a reader over the live torrent instead of
  blocking until `Complete()`, reusing `torrentArchive.CreateTorrent` and the
  event loop exactly as `Download` does.

### 4.1 Internal APIs

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
A11 origin range-download client + retry (independent; activated by Stack B's B5)
A1 + A7 + A8 ─▶ A12 stream-aware priority classes (independent; reservation-ordering only)
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
| A11 | origin range-download client + retry | `origin/blobclient/client.go` + `cluster_client.go` (+ mocks) | ~44 | inert until B5 ships the origin range endpoint |
| A12 | stream-aware priority classes | `manager.go` + `dispatcher.go` + `stream_reader.go` | ~34 | reservation ordering only; no wire/config change |

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
`time`, `sync/atomic`, `clock`, `lib/torrent/storage`, `utils/closers`.

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

	hinted atomic.Int64 // guards the demand hint — acquirePiece is called by
	                    // both Read's cursor path and ReadAt's independent-
	                    // offset path (A8), which race on the same streamReader.
	                    // Swap instead of a mutex: demand()/priority() are
	                    // already idempotent (A7), so a benign double-fire on
	                    // a rare race is harmless — no need to serialize the
	                    // call itself, only the "have I already fired" bit.
	                    // -1=none.

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
// body: pieceLen = t.PieceLength(0) iff NumPieces()>0 else 0; r := &streamReader{...};
// r.hinted.Store(-1); return r. (atomic.Int64's zero value can't be set via the
// struct literal, hence the separate Store.)

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
// streamReadahead is how many pieces to demand together, starting at the
// blocked piece itself: window is [piece, piece+streamReadahead).
const streamReadahead = 8

// demand asks the dispatcher (lazy) to fetch [lo, hi), clamped. No-op if request nil.
func (r *streamReader) demand(lo, hi int)
// body: return if request nil; clamp lo>=0, hi<=NumPieces(); return if lo>=hi;
// build []int{lo..hi-1}; r.request(pieces).
```

`acquirePiece` DELTA — on a miss, hint + demand **once** per blocked piece via an
atomic swap on `hinted` (`Read`'s cursor path and `ReadAt`'s independent-offset
path, A8, call `acquirePiece` concurrently on the same `streamReader`, and
`Swap` makes the "already hinted this piece" check-and-set atomic without a
lock — the guarded call is idempotent either way):
```go
	// inserted before the existing waitPiece() in the miss branch:
	if r.hinted.Swap(int64(piece)) != int64(piece) {
		if r.priority != nil {
			r.priority(piece)
		}
		r.demand(piece, piece+streamReadahead)
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

**Files:** `lib/dockerregistry/transfer/ro_transferer.go`; `ro_transferer_test.go`;
`lib/torrent/scheduler/scheduler.go` (`Stat`, mocks); `lib/torrent/storage/torrent_info.go`
(`TorrentInfo.Length`).
**Imports added:** `utils/closers`. **Interface:** `ImageTransferer` UNCHANGED.

```go
// scheduler.go — Scheduler interface gains a metainfo-only stat, alongside
// DownloadReader. Reuses TorrentArchive.Stat (already real, already used by
// establishIncomingHandshake, scheduler.go:424) — resolves size from cache
// metadata or the cold /kmeta sidecar without creating a torrent control,
// unlike DownloadReader (which registers the torrent and starts leeching).
Stat(namespace string, d core.Digest) (*storage.TorrentInfo, error)

func (s *scheduler) Stat(namespace string, d core.Digest) (*storage.TorrentInfo, error)
// body: return s.torrentArchive.Stat(namespace, d) — thin passthrough, no
//   event-loop round trip.

// storage/torrent_info.go — TorrentInfo has no size accessor today; add one:
func (i *TorrentInfo) Length() int64 { return i.metainfo.Length() }
// PieceLength passthrough — same pattern, used by B5's pieceRangeScope
// (§7.2) to resolve covering piece indices without creating a torrent.
func (i *TorrentInfo) PieceLength() int64 { return i.metainfo.PieceLength() }

// Stat returns blob info; on a cache miss resolves size from metainfo only
// (via the new Scheduler.Stat) — does not create a live streaming session or
// register a torrent control just to answer a size query.
func (t *ReadOnlyTransferer) Stat(namespace string, d core.Digest) (*core.BlobInfo, error)
// body: GetFileStat(d.Hex()); on os.IsNotExist||InDownloadError → info, err :=
// sched.Stat(namespace, d); return core.NewBlobInfo(info.Length());
// cache hit returns NewBlobInfo(fi.Size()).

// Download returns a reader; on a cache miss returns the streaming reader (serves
// bytes as pieces arrive) instead of blocking on the whole blob.
func (t *ReadOnlyTransferer) Download(
	namespace string, d core.Digest) (store.FileReader, error)
// body: GetFileReader(d.Hex()); on os.IsNotExist||InDownloadError → return
// sched.DownloadReader(...) directly (no adapter); cache hit returns the cached reader.
```
Mocks: `MockScheduler`/`MockReloadableScheduler` gain `Stat` (same
"mocks must land in this PR" requirement as `DownloadReader`/`ReadableRange`).

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
`...EmitsMBServed` (table over blob size), `...StatResolvesFromMetainfoOnly`
(cold `Stat` calls `sched.Stat`, never `DownloadReader`, via a call-counting
fake — zero torrent-control creation for a pure size query),
`...MultipleDownloadsOfSameBlob` (10 concurrent, `.Times(10)`); helper
`fakeBlobReader{*bytes.Reader}`. **LOC (non-test):** ~26 (`ro_transferer.go`
~16, `scheduler.go`/`torrent_info.go` ~10).

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

---

#### A11 — origin range-download client + retry

Origin's Range-read HTTP endpoint (B5) returns a `202 Accepted` "still
working, retry" response on a cold miss, the same response `downloadBlob`
already returns today. The retry mechanism for that response already exists
on the client side: `Poll` (`origin/blobclient/cluster_client.go:364`),
used by `clusterClient.DownloadBlob` (`cluster_client.go:248`). It resolves
a shuffled list of origin replicas, retries the same replica on
`http.StatusAccepted` with an exponential backoff (`defaultPollBackOff`,
`cluster_client.go:111`), and falls through to the next replica on a hard
error. `HTTPClient.DownloadBlob` (`origin/blobclient/client.go:238`) is the
single-shot HTTP call `Poll` wraps — a plain `httputil.Get` against
`/namespace/{ns}/blobs/{digest}`, returning `httputil.StatusError{Status:
202}` on a miss. The range endpoint gets a client of the same shape, wired
through the same `Poll`.

**Files:** `origin/blobclient/client.go` (`Client` interface +
`HTTPClient.DownloadBlobRange`), `origin/blobclient/cluster_client.go`
(`clusterClient.DownloadBlobRange`), `core/blob.go` (`MaxBlobRangeLength`,
shared with B5's server-side cap), generated mocks for `Client`.
**Declarations:**
```go
// origin/blobclient/client.go — Client interface gains:
DownloadBlobRange(ctx context.Context, namespace string, d core.Digest, offset, length int64, dst io.Writer) error

// HTTPClient.DownloadBlobRange — same shape as DownloadBlob (client.go:238),
// GETs /namespace/{ns}/blobs/{digest} with a Range: bytes=offset-(offset+length-1)
// header against B5's new handler instead of the whole-blob route; same
// httputil.StatusError{Status: 202} on a miss, same io.Copy(dst, r.Body) on success.
func (c *HTTPClient) DownloadBlobRange(
    ctx context.Context, namespace string, d core.Digest, offset, length int64, dst io.Writer) error

// clusterClient.DownloadBlobRange — same shape as clusterClient.DownloadBlob
// (cluster_client.go:248), plugging the single-shot call into a NEW,
// range-specific backoff (see rangePollBackOff below) instead of reusing
// defaultPollBackOff. Retries write into a scratch buffer, not dst directly
// — Poll may call the closure multiple times across origin failover, and a
// partial write from a failed attempt must not land in (or double-write to)
// the caller's dst. Rejects length up front against core.MaxBlobRangeLength
// — the same cap the server enforces (B5) — the server would 416 a larger
// request anyway, so failing fast here avoids allocating a caller-controlled
// scratch buffer before a single byte crosses the network. This is the
// client-side half of B5's range cap: B5 only bounds the server's own
// buffer, it does nothing to stop this client from allocating first.
func (c *clusterClient) DownloadBlobRange(
    ctx context.Context, namespace string, d core.Digest, offset, length int64, dst io.Writer) error {
    if length > core.MaxBlobRangeLength {
        return fmt.Errorf("range length %d exceeds max %d", length, core.MaxBlobRangeLength)
    }
    buf := bytes.NewBuffer(make([]byte, 0, length)) // bounded above; exact size known up front, no growth churn
    if err := Poll(c.resolver, c.rangePollBackOff(), d, func(client Client) error {
        buf.Reset()
        return client.DownloadBlobRange(ctx, namespace, d, offset, length, buf)
    }); err != nil {
        return err
    }
    _, err := io.Copy(dst, buf)
    return err
}
```
`core.MaxBlobRangeLength` (`core/blob.go` or similarly small, existing file —
not a new package): both `origin/blobclient` and `origin/blobserver` already
import `core` for `core.Digest`, so this is a zero-new-coupling single
source of truth instead of two independently-declared 64 MiB constants with
only a comment promising they'd stay aligned — critical-review flagged that
promise as a real drift risk (client rejects what the server would accept,
or vice versa, the moment one changes without the other). B5's
`_maxRangeLength` (§7.2) becomes a direct use of `core.MaxBlobRangeLength`
too; see that section for the corresponding one-line change.
```go
// core/blob.go (new const, no new file)
const MaxBlobRangeLength = 64 << 20 // 64 MiB; shared cap for client + server

```
`Poll` already treats "202, keep retrying" as a first-class outcome for any
request shape passed into it, so the range call reuses the same
attempt/multi-origin-fallback machinery the whole-blob path has, but with its
**own** backoff budget instead of `defaultPollBackOff`'s 15-minute one:
```go
// rangePollBackOff — sized against the caller's own client-side timeout, not
// Kraken's. A FUSE-driven range read (stargz-snapshotter) has a much
// tighter per-request timeout than a whole-blob download: containerd's
// [resolver] request_timeout_sec defaults to 30s, and stargz-snapshotter's
// own per-chunk fs blob fetch timeout (FetchTimeoutSec) defaults to 300s —
// both far below defaultPollBackOff's 900s MaxElapsedTime. Reusing the
// whole-blob backoff for ranges means Kraken silently keeps retrying long
// after the caller has already timed out and reissued its own request,
// stacking retries instead of resolving in one. 20s keeps a full retry
// cycle inside containerd's tighter 30s default with margin to spare;
// deployments with a raised request_timeout_sec can raise this too.
func (c *clusterClient) rangePollBackOff() backoff.BackOff {
    return &backoff.ExponentialBackOff{
        InitialInterval:     200 * time.Millisecond,
        RandomizationFactor: 0.05,
        Multiplier:          1.3,
        MaxInterval:         2 * time.Second,
        MaxElapsedTime:      20 * time.Second,
        Clock:               backoff.SystemClock,
    }
}
```
**Tests:** `TestHTTPClientDownloadBlobRange` (happy path, 202-pending,
404-not-found — mirrors the existing `TestHTTPClientDownloadBlob` table).
`TestClusterClientDownloadBlobRangePolls202` — a fake `Client` returning 202
N times then success, asserting `Poll` retries with backoff and eventually
succeeds, and a variant that never succeeds asserts the same "backoff timed
out on 202 responses" terminal error `DownloadBlob` already produces, within
`rangePollBackOff`'s tighter ~20s budget (not 15 minutes).
`TestClusterClientDownloadBlobRangeRejectsOversizedLength` — length just
under/at/over `core.MaxBlobRangeLength` (table): under and at succeed
(zero-byte-allocation path unaffected), over returns the error immediately
with no `Poll`/network call observed (call-counting fake `Client`
`.Times(0)`), and no buffer sized to the oversized `length` is ever
allocated.
**LOC (non-test):** ~44 (`client.go` ~15, `cluster_client.go` ~19, interface
+ mock regen ~10).

---

#### A12 — stream-aware priority classes

**Files:** `dispatch/piecerequest/manager.go`; `dispatcher.go`; `stream_reader.go`
(+ their `_test.go`). **Imports added:** none. **Depends on:** A1
(`Manager.priority`), A7 (`acquirePiece`'s hinted-miss branch), A8 (`ReadAt`).

`docs/lazy-pull-streaming-critical-review.md` finding #2 and §12 item 1 (below)
both flag that A1's `priority map[int]struct{}` is flat and ascending-sorted
only — a piece a stream is genuinely blocked on right now can lose a
reservation slot to another stream's speculative readahead (or a large
prefetch-shaped `ReadAt`) purely because of piece index. There is no wire-level
priority signal from containerd/stargz-snapshotter to Kraken to consume here
(verified: containerd's `Prepare` labels are mount-granularity, not per-range;
prefetch and on-demand are both plain Range GETs with no distinguishing
header) — but the snapshotter's own client already treats the two request
shapes very differently (`PrefetchTimeoutSec`=10s vs `FetchTimeoutSec`=300s):
one large early Range request covering the prioritized-files region (low
patience, disposable) vs. many small reactive Range requests for real FUSE
reads (high patience, must succeed). Kraken can infer the same split from
`ReadAt`'s own `len(p)` without any protocol change, and should rank the two
tiers the same way the snapshotter's own timeouts imply.

```go
// dispatch/piecerequest/manager.go

// PriorityClass ranks why a piece was prioritized. Foreground always wins a
// reservation slot over Background, regardless of piece index — a real
// blocked read must never lose its slot to another stream's speculative
// readahead or a large prefetch-shaped span.
type PriorityClass int

const (
	Background PriorityClass = iota // speculative: readahead tail, large ReadAt span
	Foreground                      // the piece a read is actually blocked on; small ReadAt span
)

// priority: was map[int]struct{}, now carries a class per piece.
priority map[int]PriorityClass

// SetPriority upgrades piece i to class if class outranks any existing
// entry; never downgrades — a piece already Foreground stays Foreground even
// if a later Background-classed span also happens to cover it.
func (m *Manager) SetPriority(i int, class PriorityClass)
// body: Lock/defer Unlock; if cur, ok := m.priority[i]; !ok || class > cur {
//   m.priority[i] = class }.

// sortedPriority returns all Foreground pieces ascending, then all
// Background pieces ascending — two groups, not one flat sort. Caller holds
// the lock.
func (m *Manager) sortedPriority() []int
// body: split m.priority into two slices by class; sort.Ints each;
// return append(foregroundSorted, backgroundSorted...).

// ReservePieces, Clear: bodies UNCHANGED from A1 — both already walk
// sortedPriority()/delete(m.priority, i) generically, independent of the
// map's value type.
```

```go
// dispatcher.go — signatures gain a class param, passed straight through.
func (d *Dispatcher) SetPriorityPiece(piece int, class piecerequest.PriorityClass)
// body: d.pieceRequestManager.SetPriority(piece, class).

func (d *Dispatcher) RequestPieces(pieces []int, class piecerequest.PriorityClass)
// body: unchanged except SetPriorityPiece(pieces[0], class).
```

```go
// stream_reader.go

// demand gains a class param, threaded through to the dispatcher.
func (r *streamReader) demand(lo, hi int, class piecerequest.PriorityClass)
// body: unchanged except r.request(pieces, class).
```
`acquirePiece`'s hinted-miss branch (A7) — the exact blocked piece is always
Foreground; the readahead tail it kicks off is Background:
```go
	if r.hinted.Swap(int64(piece)) != int64(piece) {
		if r.priority != nil {
			r.priority(piece, piecerequest.Foreground)
		}
		r.demand(piece, piece+streamReadahead, piecerequest.Background)
	}
```
`ReadAt` (A8) classifies its up-front span by size before demanding — a span
no wider than `streamReadahead` is shaped like a real on-demand FUSE read
(Foreground); a wider span is shaped like a snapshotter prefetch call
(Background). Critical-review follow-up: classifying the **whole** span
uniformly mis-tagged `lo` itself — the piece the read loop is about to
consume on its very first iteration, always imminent regardless of how wide
the rest of the span is — as Background whenever the span was wide, however
briefly (`RequestPieces` only ever prioritizes `pieces[0]` of what it's
given, so this is the one piece the class choice actually affects). The
in-loop `acquirePiece` upgrade (below) corrects it back to Foreground almost
immediately, but not before a concurrent `ReservePieces` round could
transiently observe the wrong class and skip reserving it for one cycle.
Fixed by demanding `lo` on its own, always Foreground, and only classifying
the remainder of the span by size — `lo` is never briefly Background:
```go
	class := piecerequest.Foreground
	if hi-lo > streamReadahead {
		class = piecerequest.Background
	}
	r.demand(lo, lo+1, piecerequest.Foreground) // about to be read this call; never speculative
	if hi > lo+1 {
		r.demand(lo+1, hi, class) // genuine readahead/prefetch-shaped tail
	}
```
The per-piece `acquirePiece` loop inside `ReadAt`'s read loop still
independently upgrades whichever piece is currently blocking to Foreground
via the same hinted-swap path above, so a real wait that happens to fall
inside the Background-classed tail is never left there.

**Known ceiling, not fixed here (ponytail: ceiling + upgrade path):**
priority entries are only released on piece completion (`Clear`, unchanged
from A1), not on stream abandonment — an aborted stream's claim lingers until
the piece arrives via some other path (or never, if nothing else demands it).
Fixing this needs per-stream lifecycle tracking, which would also have to
address the separately pre-existing, equally leaky `demand` bitset (A2) —
materially bigger scope than what the critical review/this fix targets.
Upgrade path: give `streamReader` a stable identity object and release its
claims on `Close()`, once real traffic shows the leak matters in practice.

**Tests:** `TestManagerSetPriorityUpgradeOnly` (table: Background then
Foreground on the same piece ⇒ Foreground; Foreground then Background ⇒
stays Foreground); `TestManagerSortedPriorityGroupsByClass` (mixed classes,
assert the full Foreground block precedes the full Background block, each
ascending); `TestDispatcherSetPriorityPieceClass`/`TestDispatcherRequestPiecesClass`
(class reaches `Manager.priority` unchanged); `TestStreamReaderAcquirePieceClassifiesBlockedPieceForeground`
(readahead tail is Background, the blocked piece itself is Foreground even
when it falls inside another span's Background-classed range);
`TestStreamReaderReadAtClassifiesBySpanSize` (table: span ≤ `streamReadahead`
⇒ Foreground, span > `streamReadahead` ⇒ Background; **and**, per the
critical-review fix above, `lo` itself is asserted Foreground via a
call-recording fake `request` even when the overall span is wide enough to
classify its tail Background — a single-call assertion that `demand` was
invoked once with `[lo, lo+1)`/Foreground and, only if `hi>lo+1`, again with
`[lo+1, hi)`/the size-derived class).
**LOC (non-test):** ~34 (`manager.go` ~15, `dispatcher.go` ~4,
`stream_reader.go` ~15).

### 5.4 What Stack A deliberately does NOT change

- **Origin** stays a whole-blob seeder for P2P purposes. On a cold first
  plain-HTTP request it materializes the full blob async (existing 202
  path); the streaming benefit here is agent↔peer P2P + warm origins. A11
  adds the agent's client for origin's range-read endpoint; the endpoint
  itself and the shared non-blocking fetch session it joins are Stack B (B5).
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
`Torrent.fetchPiece`, driven by an injected `backend.RangeDownloader`. Once
every piece is fetched and CRC-verified, the reassembled blob is promoted into
`CAStore`'s cache state (see grounding notes below) — there is no persistent
"forever partial" state on disk.

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
  this repo ships: **GCS** and **S3**. Both are cheap: production GCS's
  `transfermanager.Downloader` already accepts a `Range` field on
  `DownloadObjectInput`, and S3's `s3manager.Downloader` already accepts a
  `Range` header on `GetObjectInput` — neither needs new SDK surface, just a
  new `Client.DownloadRange` method layered on the existing downloader.
  `testfs` (devcluster-only fixture) and
  any backend lacking the capability (e.g. `hdfsbackend`) fall back to the
  unchanged whole-blob path (graceful degradation, never a regression).
- **GCS `RangeDownloader` — validated end-to-end against production.** Uber's
  real, deployed `gcsbackend` (`transfermanager.Downloader`-based, PSC-aware)
  was implemented and unit-tested against production GCS as a proof range
  reads work cleanly: `transfermanager.DownloadObjectInput` has a `Range
  *DownloadRange` field (`{Offset, Length int64}`, `Length<0` ⇒ to-EOF) that
  runs through the same worker pool, CRC verification, and single/multi-shard
  logic as whole-object `Download` (sharding only triggers past
  `DownloadPartSize`, so a normal Kraken piece is always a single shard) —
  full build + unit test pass. B1's `gcsbackend.GCSImpl.DownloadRange` targets
  this exact pipeline (§7.2 B1) — `DownloadRange` is additive on the worker
  pool `Download` already runs through, not a second SDK surface.
- Origin previously used only `*store.CAStore` (cache-only, upload+cache
  states). Stack B gives `*store.CAStore` a **third** state, `download`
  (mirroring the existing `upload` state), so cold pieces land in a sparse
  download file with per-piece `_status` metadata — but the `download` state
  shares `cache`'s backend, so a fully-fetched blob is promoted with the same
  atomic same-volume rename `MoveUploadFileToCache` already uses, into the
  **same** cache directory a warm blob would land in. There is no second
  store, no second cache directory, and therefore no way for a cold-fetched
  blob to sit duplicated once it's complete.
- Origin's P2P cold-fetch path and its plain-HTTP whole-blob path
  (`blobserver.downloadBlob`) both resolve to the **same** in-flight fetch for
  a given digest — `downloadBlob`'s cache-miss fallback now calls the
  scheduler's `Download` (blocking, whole-blob) instead of an independent
  `blobRefresher.Refresh`, which routes through the same
  `torrentControls[InfoHash]`-deduped dispatcher instance the P2P path already
  uses. A partial P2P fetch and a whole-blob HTTP request for the same digest
  therefore join one session instead of racing two.
- The `RangeDownloader` signature is `DownloadRange(ctx context.Context,
  namespace, name string, dst io.Writer, offset, length int64) error` (dst
  **before** offset/length). `AsRangeDownloader` unwraps `*ThrottledClient`,
  then type-asserts.

### 7.1 PR budget table

| PR | Scope | Files | ~LOC (non-test) | Activates? |
|----|-------|-------|------|-----------|
| B1 | `RangeDownloader` capability + GCS/S3 impls | `lib/backend/rangedownloader.go` (new) + `gcsbackend/{gcs,client}.go` + `s3backend/client.go` | ~55 | inert until B3/B4 type-assert it |
| B2 | metainfo sidecar (write at writeback) | `lib/metainfosidecar/sidecar.go` (new) + `persistedretry/writeback/executor.go` | ~60 | `/kmeta` sidecar lands on backend |
| B3 | `CAStore` download state + origin partial torrent (lazy range-fetch) | `lib/store/download_store.go` (new) + `lib/store/ca_store.go` + `originstorage/pieces.go` (new) + `originstorage/torrent.go` | ~180 (2 PRs) | partial `Torrent` fetches on demand, promotes to cache on completion |
| B4 | cold-origin wiring (both seams) | `originstorage/torrent_archive.go`, `scheduler/constructors.go`, `origin/cmd/{cmd,config}.go`, `config/origin/base.yaml`, `origin/blobserver/server.go` | ~100 | **cold origin seeds partial content** |
| B5 | `blobserver` ↔ scheduler wiring + range reads | `origin/cmd/cmd.go`, `lib/blobrefresh/refresher.go`, `origin/blobserver/server.go`, `lib/torrent/storage/torrent_info.go` | ~125 | plain HTTP whole-blob/range requests join the same fetch session as P2P, without blocking the request goroutine |
| B6 | bounded concurrent piece prefetch | `originstorage/torrent.go`, `lib/torrent/scheduler/constructors.go`, `origin/cmd/config.go`, `config/origin/base.yaml` | ~40 | `GetPieceReader` fetches the next N pieces from backend concurrently instead of one at a time |
| B3c | piece-level memcache for in-progress downloads | `lib/store/ca_store.go`, `originstorage/torrent.go` | ~55 | a piece already fetched (incl. by B6's prefetch) is served from memory to the next reader instead of a disk round-trip |

**Dependency order:** B1 and B2 are independent (different packages) and can land
in parallel. **B3 depends on B1** (the partial `Torrent` holds a
`backend.RangeDownloader`). **B4 depends on B1+B2+B3** — it fetches the B2 sidecar
through B1's `AsRangeDownloader` and constructs B3's `NewPartialTorrent`. **B5
depends on B4** — it wires the already-running scheduler (built in B4's
`origin/cmd/cmd.go` changes) into `blobserver`. **B6 depends on B3** — it
extends `Torrent`'s fetch state machine (`GetPieceReader`/`prefetchAhead`),
which B5's whole-blob drain and range handler both rely on for
`DownloadReader` to make forward progress at more than one piece at a time.
**B3c depends on B3b** (needs `ensurePiece`/`fetchPiece`/`maybePromoteToCache`
to exist) and pairs naturally with B6 (prefetched pieces are the main
beneficiary) but does not require it — it helps any two readers wanting the
same not-yet-promoted piece. B3 is the long pole; split it 2 ways: **B3a**
lifts `lib/store/download_store.go` +
`CAStore`'s third state + the `pieces.go` per-piece state model + the
`NewPartialTorrent` constructor (reports complete); **B3b** adds the lazy fetch
state machine (`GetPieceReader`/`ensurePiece`/`waitForPiece`/`fetchPiece`/
`markPieceComplete`) plus the completion→promotion step.

### 7.2 PR detail

#### B1 — `RangeDownloader` backend capability + GCS/S3 implementations

**Files:** `lib/backend/rangedownloader.go` (new — capability iface + unwrap
helper); `lib/backend/gcsbackend/{gcs,client}.go` (impl `DownloadRange`);
`lib/backend/s3backend/client.go` (impl `DownloadRange`). Callers type-assert,
so every backend lacking the capability (`hdfsbackend`, etc.) keeps working via
the whole-blob fallback — no interface break.
**Imports added:** `rangedownloader.go`: `context`, `io`. `gcsbackend`: `context`
(`storage` already imported). `s3backend`: `context` (`aws`, `s3`, `s3manager`
already imported).
**Declarations:**
```go
// lib/backend/rangedownloader.go — optional capability, sibling to Client.
// Callers MUST type-assert; backends lacking it fall back to whole-blob Download.
// ctx is request/session-scoped so a caller can abort an in-flight range
// fetch early (see B3b's Torrent.ctx) — this is a brand-new interface this
// same plan introduces, so adding ctx has no back-compat cost, unlike
// threading it through the existing, context-less backend.Client.
type RangeDownloader interface {
    DownloadRange(ctx context.Context, namespace, name string, dst io.Writer, offset, length int64) error
}

// AsRangeDownloader unwraps *ThrottledClient (which embeds but does not forward
// the method) and type-asserts. ok=false ⇒ caller falls back to whole-blob.
func AsRangeDownloader(c Client) (RangeDownloader, bool)
// body: if tc, ok := c.(*ThrottledClient); ok { c = tc.Client }; rd, ok :=
//   c.(RangeDownloader); return rd, ok.

// lib/backend/gcsbackend/gcs.go — extend the GCS interface.
type GCS interface {
    // ...existing methods unchanged...
    DownloadRange(ctx context.Context, objectName string, w io.WriterAt, offset, length int64) (int64, error)
}

// lib/backend/gcsbackend/client.go — GCSImpl.DownloadRange reuses the same
// transfermanager.Downloader pipeline whole-blob Download already runs
// through (worker pool, CRC verification, single/multi-shard logic); no
// second SDK surface. Takes the caller's ctx as the parent of the existing
// context.WithTimeout wrap, instead of the client's fixed g.ctx, so a
// caller-driven cancel (e.g. Torrent.Close, B3b) aborts this specific call
// without affecting other in-flight calls on the same GCSImpl.
func (g *GCSImpl) DownloadRange(
    ctx context.Context, objectName string, w io.WriterAt, offset, length int64) (int64, error)
// body: ctx, cancel := context.WithTimeout(ctx, DownloadTimeoutSeconds); defer
//   cancel(); in := &transfermanager.DownloadObjectInput{Bucket, Object:
//   objectName, Destination: w, Range: &transfermanager.DownloadRange{Offset:
//   offset, Length: length}, Callback: ...}; g.downloader.DownloadObject(ctx,
//   in); isObjectNotFound(err) ⇒ backenderrors.ErrBlobNotFound — same result
//   handling whole-blob Download already uses, just with Range set.

// Client.DownloadRange satisfies backend.RangeDownloader on top of GCSImpl —
// same writerAt-upcast-or-CappedBuffer handling Client.Download already uses,
// not a straight passthrough of dst.
func (c *Client) DownloadRange(
    ctx context.Context, namespace, name string, dst io.Writer, offset, length int64) error
// body: path := c.pather.BlobPath(name); writerAt, ok := dst.(io.WriterAt); if
//   !ok { writerAt = rwutil.NewCappedBuffer(c.config.BufferGuard) }; _, err :=
//   c.gcs.DownloadRange(ctx, path, writerAt, offset, length); if err != nil {
//   return err }; drain CappedBuffer into dst as Download does.

// lib/backend/s3backend/client.go — reuses the existing s3manager.Downloader;
// GetObjectInput.Range is the only new field. Uses DownloadWithContext instead
// of the context-less Download call, so ctx cancellation actually propagates.
func (c *Client) DownloadRange(
    ctx context.Context, namespace, name string, dst io.Writer, offset, length int64) error
// body: path := c.pather.BlobPath(name); writerAt fallback identical to
//   Download (io.WriterAt or rwutil.CappedBuffer); input := &s3.GetObjectInput{
//   Bucket, Key: path, Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset,
//   offset+length-1))}; c.s3.DownloadWithContext(ctx, writerAt, input);
//   isNotFound(err) ⇒ backenderrors.ErrBlobNotFound; drain CappedBuffer into
//   dst as Download does.
```

**Call-site edits:** none outside the two backend packages — `RangeDownloader`
is a capability callers discover via `AsRangeDownloader`, not a method added to
the `backend.Client` interface itself.
**Tests:** `gcsbackend/client_test.go` `TestClientDownloadRange` and
`s3backend/client_test.go` `TestClientDownloadRange` — both table-driven against
their existing mock SDK fakes: first range, interior range, length-past-EOF
clamp, not-found → `backenderrors.ErrBlobNotFound`, and a cancelled-context
case asserting the call returns promptly instead of running to completion.
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

// Executor gains a per-instance flag: sidecars only make sense for blob tasks
// (content digests), not for build-index's tag-name writeback tasks, which
// share this same Executor type.
type Executor struct {
    // ...existing fields...
    writeSidecar bool // NEW — true for origin's blob executor, false for build-index's tag executor
}

func NewExecutor(
    stats tally.Scope, fs FileStore, backends *backend.Manager,
    writeSidecar bool) *Executor

func (e *Executor) uploadMetaInfoSidecar(
    ctx context.Context, client backend.Client, t *Task) error
// body: idempotent — client.Stat(ns, metainfosidecar.Name(t.Name)) == nil ⇒
//   return nil; var tm metadata.TorrentMeta;
//   e.fs.GetCacheFileMetadata(t.Name, &tm) (os.IsNotExist ⇒ retryable error,
//   since a blob task's local TorrentMeta always exists by the time writeback
//   runs); b, _ := tm.Serialize();
//   client.Upload(ns, metainfosidecar.Name(t.Name), bytes.NewReader(b)).
```
**Call-site edits:** `executor.upload` — replace the `client.Stat`-exists early
return with a `blobExists bool`; wrap the blob `GetCacheFileReader` + `Upload` in
`if !blobExists`; then, only `if e.writeSidecar`, **always** call
`uploadMetaInfoSidecar` (so a re-push of an already-present blob still
backfills the sidecar). `origin/cmd/cmd.go` constructs its executor with
`writeSidecar=true`; `build-index/cmd/cmd.go` constructs its own executor
instance (same `writeback.Executor` type, used for tag writeback) with
`writeSidecar=false` — a tag name has no `TorrentMeta` to serialize, so
without this flag every tag writeback would hit the `os.IsNotExist` branch
and retry forever. `*store.CAStore` already implements `GetCacheFileMetadata`,
so the existing `cmd.go` wiring still satisfies the widened `FileStore`.
**Tests:** `executor_test.go` (extend) — after `Exec` with `writeSidecar=true`,
the backend holds both `name` and `name+"/kmeta"`, and the sidecar
deserializes to the local `TorrentMeta`; include the blob-already-exists case
(sidecar still written). Add a `writeSidecar=false` case using a tag-shaped
task (name with no local `TorrentMeta`) and assert `Exec` succeeds without
attempting a sidecar upload.
**LOC (non-test):** ~60 (`sidecar.go` ~14, executor `blobExists` refactor ~10,
`uploadMetaInfoSidecar` ~30, `writeSidecar` field + call-site guard ~5, iface +1).

#### B3 — origin partial torrent (lazy range-fetch) — splits B3a/B3b

**Files:** `lib/store/download_store.go` (new — third `CAStore` state,
mirroring `upload_store.go`); `lib/store/ca_store.go` (embed it, add
`MoveDownloadFileToCache`); `lib/torrent/storage/originstorage/pieces.go` (new
— per-piece status model, adapted from `agentstorage/pieces.go`);
`lib/torrent/storage/originstorage/torrent.go` (partial mode alongside the
unchanged warm `NewTorrent`).
**Imports added:** `torrent.go`: `context`, `io`, `time`, `utils/closers`, `atomic`
(`bitset` present); `pieces.go`: `sync`, `lib/store/metadata`.
**Declarations (B3a — `CAStore` third state + state model + constructor):**
```go
// lib/store/download_store.go — third CAStore state, mirrors upload_store.go.
// Shares cacheStore's backend so promotion is a same-volume rename, not a copy.
type downloadStore struct {
    state                        base.FileState
    backend                      base.FileStore
    readPartSize, writePartSize  int
}
func newDownloadStore(dir string, backend base.FileStore, readPartSize, writePartSize int) (*downloadStore, error)
func (s *downloadStore) CreateDownloadFile(name string, length int64) error
func (s *downloadStore) GetDownloadFileReadWriter(name string) (FileReadWriter, error)
func (s *downloadStore) GetDownloadFileMetadata(name string, md metadata.Metadata) error
func (s *downloadStore) SetDownloadFileMetadata(name string, md metadata.Metadata) error
func (s *downloadStore) DeleteDownloadFile(name string) error

// lib/store/ca_store.go — CAStore gains the third state, only when configured.
type CAStore struct {
    // ...existing fields...
    *downloadStore // NEW, nil when config.DownloadDir == ""
}
// newCAStore: if config.DownloadDir != "", construct downloadStore sharing
// cacheStore's backend, and cleanup.addJob("download", config.DownloadCleanup,
// downloadStore.newFileOp()). Every existing origin deployment (DownloadDir
// unset) and the agent (which doesn't use CAStore) are unaffected.

// Invariant — complete-blob visibility. GetCacheFileReader, GetCacheFileStat,
// and GetCacheFileMetadata must resolve cache-state and memcache only, keyed
// by bare digest.Hex(), and must NEVER resolve a download-state file, at any
// point before promotion. This already holds today by construction: all
// three check s.memCache then fall through to s.cacheStore
// (ca_store.go:458,470,491) and never touch anything download-state-shaped;
// downloadStore's methods are named distinctly (GetDownloadFileReadWriter,
// etc.), so there is no accidental override surface. Do NOT add a "try
// download state on cache miss" branch to any of the three, and do not widen
// their key handling to accept anything but a bare digest — either change
// lets a whole-blob reader see an incomplete sparse file under the
// content-addressed digest, which is exactly the bug the one-store,
// third-state redesign exists to prevent.

// MoveDownloadFileToCache commits a fully-fetched download file as cacheName,
// verifying its digest first — same shape as the existing MoveUploadFileToCache.
// Idempotent: if downloadName is already gone (a prior call already promoted
// it), treat as success rather than a not-found error.
func (s *CAStore) MoveDownloadFileToCache(downloadName, cacheName string) error
// body: if the download-state file is already gone (os.IsNotExist) return nil
//   (already promoted); else verify digest against a download-state reader,
//   then cacheStore.newFileOp().MoveFileFrom(cacheName, cacheStore.state,
//   downloadPath) — atomic os.Rename, same backend as MoveUploadFileToCache.
//   Also deletes the piece-status sidecar metadata. Does not itself populate
//   memCache (matches MoveUploadFileToCache's existing behavior) — a promoted
//   blob is read back from disk via cacheStore until something else warms it.

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
    d core.Digest, cas *store.CAStore, numPieces int) ([]*piece, int, error)
// body: GetOrSetMetadata(_status) seeded empty; rebuild []*piece + completed
//   count; tolerate cas's "in cache" error (already promoted).

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
    rd          backend.RangeDownloader // NEW
    namespace   string                  // NEW
    pieces      []*piece                // NEW
    ctx         context.Context         // NEW — fetch-lifetime cancellation, see below
    cancel      context.CancelFunc      // NEW
    fetchSem    chan struct{}           // NEW — B6, bounds concurrent prefetch
}
func NewPartialTorrent(
    cas *store.CAStore, rd backend.RangeDownloader,
    namespace string, mi *core.MetaInfo) (*Torrent, error)
// body: ctx, cancel := context.WithCancel(context.Background()); cache-hit short-circuit first — cas.GetCacheFileStat(digest) == nil ⇒
//   return NewTorrent(cas, mi) (warm), no download state touched at all. This
//   check already covers a digest warm only in memcache, since
//   GetCacheFileStat itself checks memCache before falling to disk — a
//   memcache-only-warm digest already never creates download state. If this
//   check is ever changed, it must keep going through GetCacheFileStat (or
//   GetCacheFileReader), not a disk-only variant, or that guarantee breaks
//   silently. Otherwise: cas.CreateDownloadFile(mi.Digest().Hex(), mi.Length())
//   tolerating "already exists"/"in cache" errors; pieces, completed, err :=
//   restorePieces(...); numComplete = completed (the real count restorePieces
//   already computed from persisted piece status — NOT mi.NumPieces(), which
//   would report every piece complete before any has been fetched). Warm
//   NewTorrent(cas, mi) unchanged.
```
B3a keeps the reported-complete invariant: `Complete()=true`, `HasPiece=true`,
`Bitfield()=Complement()`, `MissingPieces()=[]` — unchanged from today, so the
torrent advertises every piece and the dispatcher asks for any of them on demand.
**Declarations (B3b — lazy fetch state machine + completion promotion):**
```go
func (t *Torrent) GetPieceReader(pi int) (storage.PieceReader, error)
// body: partial ⇒ ensurePiece(pi) then t.prefetchAhead(pi) (B6, fire-and-forget)
//   then NewFileReader(getFileOffset, PieceLength, &downloadOpener{t}); warm ⇒
//   NewFileReader(..., &opener{t}) (unchanged).
func (t *Torrent) ensurePiece(pi int) error
// body: fast-path p.complete(); tryMarkDirty(): complete⇒nil, dirty⇒
//   waitForPiece(p), else elected fetcher ⇒ fetchPiece (on err markEmpty) ⇒
//   markPieceComplete ⇒ maybePromoteToCache.
func (t *Torrent) waitForPiece(p *piece) error
// body: spin-poll p.snapshot() every _partialFetchPollInterval; _complete⇒nil,
//   _empty⇒err, deadline _partialFetchTimeout⇒err.
func (t *Torrent) fetchPiece(pi int) error
// body: f := cas.GetDownloadFileReadWriter(digest); f.Seek(getFileOffset(pi));
//   h := core.PieceHash(); rd.DownloadRange(t.ctx, namespace, digest,
//   io.MultiWriter(f, h), getFileOffset(pi), PieceLength(pi)); if h.Sum32() !=
//   metaInfo.GetPieceSum(pi) ⇒ errors.New("invalid piece sum"). t.ctx is the
//   Torrent's lifetime context (see the fetch-cancellation note below) — an
//   in-flight fetch aborts if the Torrent is torn down mid-fetch instead of
//   running to completion against an abandoned session.
func (t *Torrent) markPieceComplete(pi int) error
// body: cas.Download().SetMetadataAt(digest, &pieceStatusMetadata{},
//   []byte{byte(_complete)}, int64(pi)); pieces[pi].markComplete() only on the
//   first complete⇒complete transition for pi does it call t.numComplete.Inc()
//   — tryMarkDirty's single-fetcher-per-piece guarantee makes this call once
//   per piece, so no separate CAS is needed on the counter itself.
func (t *Torrent) maybePromoteToCache() error
// body: if t.numComplete.Load() != int32(len(t.pieces)) { return nil };
//   cas.MoveDownloadFileToCache(digest, digest) — verify + atomic rename into
//   the shared cache state. No extra guard needed against concurrent callers
//   (the last two pieces completing on different goroutines both pass the
//   numComplete check): MoveDownloadFileToCache is already idempotent (B3a)
//   — the second caller finds the download-state file gone and returns nil.
//   A sync.Once here would only save a redundant digest-verify+stat on the
//   rare double-call; not worth a new field for that. On success this
//   Torrent's download-side state no longer exists (subsequent reads for
//   this digest hit warm NewTorrent).

// RangeReadable is an optional capability a Torrent may implement to answer
// "is this specific byte span locally readable right now" without lying
// through HasPiece/Bitfield/Complete, which stay hardcoded complete for
// partial torrents (B3a's as-built model, so origin can seed P2P
// immediately) — those three answer a different question ("can I be asked
// for any piece") than ReadableRange ("has this piece actually landed").
// Type-asserted by callers, same discovery pattern as
// backend.AsRangeDownloader (B1).
type RangeReadable interface {
    ReadableRange(offset, length int64) bool
}
func (t *Torrent) ReadableRange(offset, length int64) bool
// body: !t.partial ⇒ true (warm is always readable); else every piece index
//   covering [offset, offset+length) has t.pieces[pi].complete() == true.

// Fetch-lifetime cancellation. t.ctx/t.cancel (added to the Torrent struct
// above) are threaded into every fetchPiece call, so an in-flight
// DownloadRange aborts the moment the Torrent is torn down instead of
// running to completion (bounded only by the backend's own generous,
// whole-blob-scoped timeout) against a session nothing is reading from
// anymore.

// Close cancels any in-flight/future fetches for this Torrent. Type-asserted
// by TorrentArchive.DeleteTorrent (B4) — same optional-capability discovery
// pattern as AsRangeDownloader/RangeReadable — so warm Torrents, which don't
// implement it, are unaffected.
func (t *Torrent) Close() error
// body: t.cancel(); return nil.

type downloadOpener struct{ torrent *Torrent }
func (o *downloadOpener) Open() (store.FileReader, error)
// body: cas.GetDownloadFileReadWriter(digest) (read-only view).
```
**Call-site edits:** none outside `originstorage`/`lib/store` until B4 — the
warm `NewTorrent(cas, mi)` signature is unchanged, so existing construction
still compiles.
**Tests:** `ca_store_test.go` (extend) — `TestCAStoreMoveDownloadFileToCache`
(happy path, digest-mismatch rejection, double-promote no-op), plus a
`download` cleanup-job TTL test mirroring the existing `upload`/`cache` job
tests. `torrent_test.go` (extend) against a real `*store.CAStore` (with
`DownloadDir` configured) fixture + a fake in-memory `RangeDownloader`: (a)
first `GetPieceReader(pi)` does exactly one `DownloadRange`, correct bytes;
second call zero further fetches; (b) sum mismatch ⇒ error + re-fetchable; (c)
N concurrent goroutines on one piece ⇒ exactly one `DownloadRange`; (d) restart
durability via a second Torrent over the same `cas`; (e) short last piece
length; (f) once every piece completes, the blob is readable from
`cas.GetCacheFileReader` and the download-side file/metadata is gone; (g)
`NewPartialTorrent` on a digest already warm in cache never creates a download
file or calls `DownloadRange`; (h) `NewPartialTorrent` over a `cas` with N of
M pieces already persisted reports `numComplete == N`, not `M`, immediately
after construction; (i) two goroutines completing the last two pieces
concurrently each call `MoveDownloadFileToCache` and both return nil (second
call's idempotent no-op path), blob ends up correctly promoted exactly once;
(j) for a digest with N of M pieces persisted in download state (not yet
promoted), `cas.GetCacheFileReader`/`GetCacheFileStat`/`GetCacheFileMetadata`
all return a miss, throughout — this must hold immediately after each of the
N piece completions, not just at N=0; only after `MoveDownloadFileToCache`
succeeds do these three flip to hits; (k) `NewPartialTorrent` for a digest
present only in (whole-blob) memcache short-circuits to warm `NewTorrent`
and never calls `CreateDownloadFile`; (l) `ReadableRange` reports false for a
span with any not-yet-complete covering piece and true once every covering
piece is complete, independent of `HasPiece`/`Bitfield`/`Complete` (which
stay "complete" throughout — asserting these are genuinely answering
different questions); (m) a fake `RangeDownloader` that blocks until
`ctx.Done()` proves `fetchPiece` aborts the moment `Torrent.Close()` runs,
and a `DeleteTorrent` test asserts `Close()` fires for partial torrents and
is never called for warm ones.
**LOC (non-test):** ~180 across 2 PRs (`download_store.go` + `ca_store.go`
~35, `pieces.go` ~80, `torrent.go` constructor + state machine + promotion
~65 [B3a ~25 / B3b ~40]).

#### B3c — piece-level memcache for in-progress downloads

Today a piece fetched during a cold download only ever lands on disk (the
sparse download-state file) until promotion, so a second concurrent reader
of the same not-yet-promoted piece — a P2P peer request, a range read, or
B6's own prefetch feeding a reader that later catches up to it — pays a disk
read for bytes the process fetched moments ago. This gives in-progress
pieces a memory cache too, without touching the complete-blob visibility
invariant above.

**Files:** `lib/store/ca_store.go` (3 new thin methods + a key helper);
`lib/torrent/storage/originstorage/torrent.go` (`fetchPiece` populates the
cache, `GetPieceReader` consults it, `maybePromoteToCache` purges it).
**Imports added:** `ca_store.go`: `strconv` (`core` present).
**Reuses the existing `s.memCache` instance** — its API (`Get`/`Add`/`Remove`/
`TryReserve`/`ReleaseReservation`, TTL sweep via `GetExpiredEntries`+
`RemoveBatch`) is keyed by a plain string with no assumption it's a digest.
Piece keys always carry a `#<index>` suffix; the three visibility-sensitive
lookups (`GetCacheFileReader`/`Stat`/`Metadata`) only ever call
`s.memCache.Get(digest.Hex())` — a bare digest string can never equal a
`digest#index` string, so the two key spaces can never collide in the same
map. This is what lets piece caching reuse `s.memCache`'s existing
config/capacity/TTL machinery instead of standing up a second cache with its
own knob — and it is also the safety argument for the invariant above, so
don't "simplify" piece keys back to a bare digest string.
**Declarations:**
```go
// lib/store/ca_store.go
func pieceCacheKey(digest core.Digest, pi int) string {
    return digest.Hex() + "#" + strconv.Itoa(pi)
}

// GetPieceFromCache returns a piece's bytes if cached, else (nil, false).
func (s *CAStore) GetPieceFromCache(digest core.Digest, pi int) ([]byte, bool)
// body: if s.memCache == nil return nil, false; e := s.memCache.Get(pieceCacheKey(digest, pi));
//   if e == nil return nil, false; return e.Data, true.

// PutPieceInCache best-effort caches a freshly-verified piece. Never blocks
// or errors the fetch path — same fire-and-forget posture the existing
// addToMemoryCache reservation pattern already uses for whole blobs.
func (s *CAStore) PutPieceInCache(digest core.Digest, pi int, data []byte)
// body: if s.memCache == nil return; if !s.memCache.TryReserve(uint64(len(data))) return;
//   if !s.memCache.Add(&cache.MemoryEntry{Name: pieceCacheKey(digest, pi), Data: data,
//   CreatedAt: s.clk.Now()}) { s.memCache.ReleaseReservation(uint64(len(data))) }.

// PurgePieceCache drops every cached piece for digest — called once the blob
// promotes, freeing memory the moment pieces are redundant (the whole blob
// is now servable warm via the normal cacheStore/memCache-by-digest path).
func (s *CAStore) PurgePieceCache(digest core.Digest, numPieces int)
// body: if s.memCache == nil return; for pi := 0; pi < numPieces; pi++ {
//   s.memCache.Remove(pieceCacheKey(digest, pi)) }. The existing TTL sweep is
//   the fallback for pieces belonging to a torrent abandoned before
//   completing — same pattern as today's memCache cleanup job, no new job.

// CacheEnabled reports whether piece/blob memcache is configured at all.
// Critical-review follow-up: fetchPiece (below) used to build its in-memory
// verify buffer unconditionally, paying an allocation + copy per piece even
// with memcache off entirely — this lets it skip that work outright rather
// than build a buffer nothing will ever read.
func (s *CAStore) CacheEnabled() bool
// body: return s.memCache != nil.

// originstorage/torrent.go
func (t *Torrent) fetchPiece(pi int) error
// body: unchanged network fetch + CRC verify, except: if t.cas.CacheEnabled(),
//   the verify writer becomes io.MultiWriter(f, h, &buf) instead of
//   io.MultiWriter(f, h) — the verified bytes are already in memory — and on
//   success t.cas.PutPieceInCache(t.metaInfo.Digest(), pi, buf.Bytes())
//   (best-effort, return value ignored — a cache miss later just means one
//   disk read); when the cache is disabled, unchanged io.MultiWriter(f, h),
//   no buffer allocated or copied into at all.

func (t *Torrent) GetPieceReader(pi int) (storage.PieceReader, error)
// body: partial ⇒ ensurePiece(pi); if b, ok := cas.GetPieceFromCache(digest, pi); ok
//   { return piecereader.NewBuffer(b), nil } (existing type,
//   lib/torrent/storage/piecereader/buffer.go, no new reader needed) — else
//   fall back to the existing NewFileReader(..., &downloadOpener{t}) disk path
//   (cache miss/evicted/disabled); then t.prefetchAhead(pi) (B6, unchanged).
//   Net effect: a piece B6 already prefetched in the background is served
//   straight from memory once the reader's cursor reaches it.

func (t *Torrent) maybePromoteToCache() error
// body: unchanged promotion via MoveDownloadFileToCache, plus on success
//   t.cas.PurgePieceCache(t.metaInfo.Digest(), len(t.pieces)).
```
**No new config.** Reuses `config.MemoryCache.Enabled`/capacity/TTL as-is —
one shared budget for whole-blob and piece entries. Not adding a second
capacity config until real traffic shows piece entries need independent
sizing from whole-blob entries.

**Known ceiling (critical-review follow-up), not fixed here:**
`CacheEnabled()` only skips the buffer when the cache is off entirely; a
cache that's on but momentarily at capacity still pays one wasted
buffer-allocate-and-copy per piece, since `PutPieceInCache`'s own
`TryReserve` check only happens after `fetchPiece` already built the buffer.
Avoiding that too means reserving capacity for `PieceLength(pi)` (already
known before the fetch starts) up front and only wiring in the third
`MultiWriter` arm if the reservation succeeds — a real fix, but a bigger
restructuring of `fetchPiece`/`PutPieceInCache`'s split responsibilities than
this pass justifies; capacity-rejection is expected to be the occasional
case, not the steady state, unlike cache-disabled-entirely which is a
static, common configuration.
**Tests:** `ca_store_test.go` (extend) — `TestPieceCacheRoundTrip` (put then
get, two different piece indices for the same digest don't collide,
capacity-full put is a silent no-op); `TestPieceCacheKeyNeverCollidesWithDigestKey`
— for every `(digest, pi)`, `pieceCacheKey(digest, pi) != digest.Hex()`, and
`GetCacheFileReader(digest.Hex())` never returns a piece entry even when one
is cached for that digest; `TestCacheEnabled` (nil `memCache` ⇒ false,
configured ⇒ true). `torrent_test.go` (extend) — a piece fetched once
and read twice triggers exactly one `DownloadRange` and one disk write, the
second `GetPieceReader` call served via `piecereader.NewBuffer`; after
promotion, every piece key for that digest is gone from `memCache`;
`TestFetchPieceSkipsBufferWhenCacheDisabled` — a `CacheEnabled()==false`
fake asserts `PutPieceInCache` is never called and (via a wrapped writer
that fails the test if written beyond `f`/`h`) no third writer ever receives
bytes.
**LOC (non-test):** ~55 (`ca_store.go` ~33, `torrent.go` ~22).

#### B4 — cold-origin wiring (both seams)

**Files:** `originstorage/torrent_archive.go` (cold metainfo + partial torrent
selection); `lib/torrent/scheduler/constructors.go` (`NewOriginScheduler`
params); `origin/cmd/config.go` + `origin/cmd/cmd.go` (`CAStore`'s
`DownloadDir`/`DownloadCleanup` config, pass `backendManager`);
`config/origin/base.yaml` (`castore.download_dir`/`download_cleanup`);
`origin/blobserver/server.go` (HTTP metainfo cold branch).
**Imports added:** `torrent_archive.go`: `lib/metainfosidecar`; `server.go`:
`lib/metainfosidecar` (`backend` present).
**Declarations:**
```go
// torrent_archive.go — archive holds only cas (no second store) + backends.
type TorrentArchive struct {
    cas           *store.CAStore
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
// GetTorrent: rd != nil ⇒ NewPartialTorrent(cas, rd, namespace, mi) (which
//   itself short-circuits to warm NewTorrent if cas already has the blob
//   cached — see B3a); else NewTorrent(cas, mi). Stat: loadMetaInfo (ignore
//   rd) + complement bitfield. DeleteTorrent (existing TorrentArchive
//   interface method, storage.go:64): unchanged lookup/removal, plus type-
//   asserts the removed torrent for an unexported `closer interface { Close()
//   error }` and calls Close() if present — same optional-capability
//   discovery pattern as AsRangeDownloader/RangeReadable, so this doesn't
//   widen the storage.Torrent interface for agentstorage, which never
//   implements it. This is what actually cancels a partial Torrent's
//   in-flight fetches (B3b's Torrent.ctx) on teardown.

// origin/blobserver/server.go — getMetaInfo cold branch.
func (s *Server) coldMetaInfoFromSidecar(
    namespace string, d core.Digest) (*core.MetaInfo, bool)
// body: same shape as coldMetaInfo: GetClient → AsRangeDownloader (capability
//   gate) → metainfosidecar.Fetch. getMetaInfo, on os.IsNotExist, tries this and
//   returns mi.Serialize() (200) before falling back to startRemoteBlobDownload.
```
**Call-site edits:**
- `scheduler/constructors.go NewOriginScheduler` gains `backends
  *backend.Manager`, passing it (and `cas`) to
  `originstorage.NewTorrentArchive(cas, backends, blobRefresher)` — no `cads`
  parameter, since there's no second store to construct or thread through.
- `origin/cmd/config.go`: `CAStoreConfig` gains `DownloadDir string` and
  `DownloadCleanup CleanupConfig` (§7.2 B3a) — no new top-level config type.
- `origin/cmd/cmd.go`: drops the `store.NewCADownloadStore(...)` call
  entirely; `cas` (already constructed for the warm path) now also backs cold
  fetches once `config.CAStore.DownloadDir` is set.
- `config/origin/base.yaml`: add `download_dir`/`download_cleanup` under the
  existing `castore:` block — no new top-level `cadownloadstore:` block, and
  no need to worry about it colliding with `castore.cache_dir` since it's the
  same store instance.
**Tests:** `torrent_archive_test.go` (extend) — cold digest with a sidecar on a
fake in-memory `backend.Client`+`RangeDownloader` fixture ⇒ `GetTorrent`
returns a partial torrent and `Stat` the complement bitfield; no sidecar /
non-range backend (a fake `backend.Client` that does not implement
`RangeDownloader`) ⇒ falls back to `blobRefresher.Refresh` (error), proving
graceful degradation.
**LOC (non-test):** ~90 (`torrent_archive.go` ~45, `blobserver` ~22,
`constructors`/`cmd`/`config`/yaml ~23).

#### B5 — `blobserver` ↔ scheduler wiring + range reads

Origin already runs its own P2P scheduler (`origin/cmd/cmd.go`,
`scheduler.NewOriginScheduler`, wired in B4) backed by the same
`originstorage.TorrentArchive`/`cas` as the HTTP blob server, but
`blobserver.New` never receives that scheduler. `downloadBlob`'s cache-miss
path falls back to an independent `blobRefresher.Refresh` (whole-blob), with
no visibility into whatever the scheduler's dispatcher is already doing for
that digest via P2P — a P2P piece fetch and a plain HTTP GET for the same
cold digest run as two uncoordinated fetches against the backend. This PR
routes both onto the scheduler's shared fetch session.

Depends on Stack A's A11: the Range-read handler below returns the same
`202 Accepted` "pending, retry" response `downloadBlob` already returns on a
cold miss, which only works end-to-end once a client retries on it — A11
adds that client (`clusterClient.DownloadBlobRange`, wired through the same
`Poll` helper `DownloadBlob` already uses).

`blobRefresher.Refresher` gains a generic entrypoint alongside `Refresh`,
reusing its existing dedup registry (`r.requests *dedup.RequestCache`,
`lib/blobrefresh/refresher.go`) — `RequestCache.Start(id string, ...)` takes
an arbitrary caller-chosen string, so the id isn't limited to a bare digest:
```go
// lib/blobrefresh/refresher.go
// TriggerBackground's dedup id is digest + an optional scope, not bare
// digest — scope="" preserves exactly today's whole-blob-vs-whole-blob
// dedup (same id Refresh already uses); a non-empty scope (a covering
// piece range, see pieceRangeScope below) lets disjoint spans of the same
// digest, or a whole-blob request and a range request, proceed
// independently instead of colliding on one slot. Correctness is
// unaffected either way: tryMarkDirty (B3b) is the actual per-piece dedup
// backstop regardless of how many TriggerBackground goroutines are
// concurrently trying to reach a piece — this only removes an
// orchestration-layer serialization point that doesn't correspond to any
// real resource constraint. RequestCache's existing NumWorkers/BusyTimeout
// (unchanged) already bounds worst-case goroutine fan-out from the wider
// key space; no new concurrency control needed.
func (r *Refresher) TriggerBackground(d core.Digest, scope string, fn func() error) error {
    id := d.Hex()
    if scope != "" {
        id += ":" + scope
    }
    return r.requests.Start(id, fn)
}
```
`downloadBlob` and a new Range-read handler both use it to run their backend
work off the request goroutine, joining the same live `*Torrent` P2P uses via
`sched.DownloadReader` (which does not depend on `Complete()` — a cold
partial `Torrent` reports `Complete()=true` from construction per B3a's
as-built model, so `sched.Download`'s blocking wait is not usable here).

**Files:** `origin/cmd/cmd.go` (pass `sched` into `blobserver.New`);
`lib/blobrefresh/refresher.go` (`TriggerBackground`); `origin/blobserver/server.go`
(`downloadBlob` cache-miss path + new Range-read handler + `pieceRangeScope`);
`lib/torrent/scheduler/scheduler.go` (`ReadableRange`, mocks);
`lib/torrent/storage/torrent_info.go` (`PieceLength`).
**Declarations:**
```go
// origin/blobserver/server.go
type Server struct {
    // ...existing fields...
    sched scheduler.Scheduler // NEW
}

// _maxRangeLength: was a locally-declared 64 MiB constant; now a direct
// alias of core.MaxBlobRangeLength (A11, §5.3) — single source of truth
// shared with the client's own cap, closing the critical-review drift risk
// of two independently-declared constants with only a comment keeping them
// aligned. Same reasoning as before (generous multiple of typical piece
// size + readahead span, hardcoded, not a config knob until real traffic
// shows a different cap is needed — same posture B3c used to skip a new
// config field). Bounds the range copy/drain below.
const _maxRangeLength = core.MaxBlobRangeLength

func (s *Server) downloadBlob(namespace string, d core.Digest, dst io.Writer) error
// body: cache-hit fast path unchanged (cas.GetCacheFileReader). On miss (and
//   only when the namespace's backend has a RangeDownloader + sidecar, same
//   gate as B4's coldMetaInfo — otherwise fall through to the unchanged
//   blobRefresher.Refresh):
//     err := s.blobRefresher.TriggerBackground(d, "", func() error {
//         br, err := s.sched.DownloadReader(namespace, d)
//         if err != nil { return err }
//         defer br.Close()
//         _, err = io.Copy(io.Discard, br) // drains every piece through it
//         return err
//     })
//     // ErrPending -> "retry later" response, same as today
//   The background work runs through the same shared *Torrent/tryMarkDirty
//   machinery P2P uses (B3b), so a concurrent P2P fetch for the same digest
//   never duplicates backend work. Once every piece lands, B3b's
//   maybePromoteToCache fires and the blob is warm for every future request.

func (s *Server) downloadBlobRangeHandler(w http.ResponseWriter, r *http.Request) error
// body: parse the Range header; length > _maxRangeLength ⇒ 416 Range Not
//   Satisfiable, no fetch triggered. Otherwise, check whether the piece(s)
//   this range needs are already fetched via the sanctioned
//   s.sched.ReadableRange(namespace, d, offset, length) (new Scheduler
//   method, see below) — NOT by reaching into originstorage's piece status
//   directly. HasPiece/Bitfield() stay hardcoded "complete" from
//   construction (B3a's as-built model, so origin can seed P2P peers
//   immediately); ReadableRange answers a different question ("has this
//   span actually landed") without touching that lie. If ready, serve
//   directly off the local pieces with no backend call and no session
//   setup — CopyReadyRange (new Scheduler method, see below) skips
//   DownloadReader's CreateTorrent+event-loop+streamReader machinery
//   entirely, since ReadableRange already proved the bytes are local:
//     if ready {
//         return s.sched.CopyReadyRange(namespace, d, w, offset, length)
//     }
//   On a miss, trigger the fetch and return immediately, reusing
//   TriggerBackground with a closure scoped to the range — pieceRangeScope
//   (below) resolves the covering piece indices so disjoint ranges of the
//   same digest register independent slots instead of colliding on one:
//     scope := pieceRangeScope(s.sched, namespace, d, offset, length)
//     err := s.blobRefresher.TriggerBackground(d, scope, func() error {
//         br, err := s.sched.DownloadReader(namespace, d)
//         if err != nil { return err }
//         defer br.Close()
//         // Drains the span to trigger the fetch; discards the bytes since
//         // this closure only needs the fetch to happen, not to serve them
//         // (the retry that follows takes the ready branch above once it
//         // lands). No full-length buffer — bounded by _maxRangeLength above.
//         _, err = io.CopyN(io.Discard, io.NewSectionReader(br, offset, length), length)
//         return err
//     })
//     // ErrPending -> "retry later" response, same shape as downloadBlob's
//   ReadAt/demand() (lib/torrent/scheduler/stream_reader.go) requests only
//   the piece span for [offset, offset+length) plus a small bounded
//   readahead, never the whole blob. Both closures register through the
//   same lazy, demand-driven dispatcher — streamTorrentEvent.apply calls
//   ctrl.dispatcher.SetLazy() (scheduler/events.go) so registering a stream
//   reader never fetches anything beyond what's actually read; the
//   whole-blob closure drains everything because io.Copy reads sequentially
//   to EOF, not because DownloadReader forces it.

// scheduler.go — Scheduler interface gains, alongside DownloadReader (A6):
ReadableRange(namespace string, d core.Digest, offset, length int64) (bool, error)
CopyReadyRange(namespace string, d core.Digest, w io.Writer, offset, length int64) error

func (s *scheduler) CopyReadyRange(
    namespace string, d core.Digest, w io.Writer, offset, length int64) error
// body: t, err := s.torrentArchive.GetTorrent(namespace, d); if err != nil {
//   return err }; walk piece indices covering [offset, offset+length) via
//   t.PieceLength(pi); for each: pr, err := t.GetPieceReader(pi); skip
//   leading bytes in a boundary piece via io.CopyN(io.Discard, pr, skip);
//   io.CopyN(w, pr, wanted); pr.Close(). Same GetTorrent lookup
//   ReadableRange already does (no CreateTorrent, no event-loop send/
//   receive, no streamReader allocated) — GetPieceReader resolves to
//   whichever of B3b's disk path or B3c's memcache path already has the
//   piece; the caller doesn't need to know which.

func (s *scheduler) ReadableRange(
    namespace string, d core.Digest, offset, length int64) (bool, error)
// body: t, err := s.torrentArchive.GetTorrent(namespace, d); if err != nil {
//   return false, err }; if rr, ok := t.(storage.RangeReadable); ok {
//   return rr.ReadableRange(offset, length), nil }; return t.Complete(), nil
//   — agent/warm torrents don't implement RangeReadable and don't need to:
//   Complete() is already truthful for them. This is a direct, synchronous
//   call (no event-loop round trip) — same shape as the existing
//   torrentArchive.Stat used by handshakes (scheduler.go:424).

// server.go — pieceRangeScope resolves the covering piece indices for
// [offset, offset+length) via Scheduler.Stat (fix #2: metainfo-only, no
// torrent/event-loop round trip, works cold off B4's sidecar) and formats
// them as a TriggerBackground scope key. Falls back to an empty scope
// (today's coarse, digest-only behavior) if Stat itself fails — that only
// happens when the digest can't be resolved at all, an error path that
// fails the request regardless.
func pieceRangeScope(sched scheduler.Scheduler, namespace string, d core.Digest, offset, length int64) string
// body: info, err := sched.Stat(namespace, d); if err != nil { return "" };
//   pl := info.PieceLength(); first := offset / pl; last := (offset +
//   length - 1) / pl; return fmt.Sprintf("%d-%d", first, last).
```
Mocks: `MockScheduler`/`MockReloadableScheduler` gain `ReadableRange` and
`CopyReadyRange` (same "mocks must land in this PR" requirement A6 already
uses for `DownloadReader` — an interface method addition breaks every
consumer until mocks satisfy it).

`downloadBlob` and the range handler share one skeleton: a cheap local
check, else trigger-and-return, so neither blocks an HTTP handler goroutine
on backend I/O. `DownloadReader` resolves to the same live `*Torrent` for a
digest regardless of which of the three entry points (P2P, whole-blob HTTP,
range HTTP) asked for it first, and per-piece fetches are deduped via
`tryMarkDirty` (B3b) within that shared instance, so none of the three
re-fetches a piece another one already completed.

`TriggerBackground`'s dedup id is now digest + covering piece range
(`pieceRangeScope`, above), not bare digest: a whole-blob request
(`scope=""`) and a range request, or two range requests for disjoint piece
spans of the same digest, register independent slots instead of colliding
on one. A snapshotter-driven workload issuing many small, scattered range
reads against the same layer in a short window — TOC lookups, individual
file chunks — no longer serializes those requests through a single
per-digest gate one retry round-trip at a time. Requests whose piece ranges
are identical or overlapping still coalesce onto one goroutine (unchanged
behavior for that case — avoids redundant `DownloadReader` sessions for
truly-repeated requests). `tryMarkDirty`'s piece-level dedup is unaffected
and remains the actual correctness backstop regardless of how many
`TriggerBackground` goroutines are concurrently in flight for a digest —
this change is purely an orchestration-layer fix, reusing
`dedup.RequestCache`'s already-arbitrary string key and its existing
`NumWorkers`/`BusyTimeout` bound (no new concurrency-control code).
**Call-site edits:** `origin/cmd/cmd.go` — reorder `blobserver.New(...)`'s
call to happen after `sched` is constructed (it already does) and add `sched`
to the parameter list. Register the new Range-read handler alongside the
existing `/namespace/{namespace}/blobs/{digest}` route.
**Tests:** `blobrefresh_test.go` — `TestRefresherTriggerBackgroundDedups`: same
scope (including two bare-digest/whole-blob calls, `scope=""`) ⇒ second call
gets `ErrRequestPending`, no second goroutine spawned; disjoint scope for the
same digest ⇒ both proceed concurrently (observed via a fake `fn` that
blocks on a channel until released); a whole-blob call (`scope=""`) and a
range call (non-empty scope) for the same digest also run independently.
`TestPieceRangeScope` — table: offset/length within one piece, spanning a
piece boundary, `Scheduler.Stat` error ⇒ `""`.
`blobserver_test.go` — a partial P2P fetch for digest D (some pieces
complete, via a fake dispatcher/scheduler) followed by a concurrent
`downloadBlob` call for D asserts (a) `downloadBlob` returns immediately
without blocking the calling goroutine, (b) no second/duplicate backend
download is triggered (call-counting fake `RangeDownloader`), (c) once the
background drain completes, the blob is warm and a subsequent `downloadBlob`
serves it directly, (d) while the P2P fetch has some-but-not-all pieces
complete (and therefore cached under piece keys per B3c), the concurrent
`downloadBlob`'s cache-hit fast path (`cas.GetCacheFileReader`) must not
observe a hit — it only becomes a hit after every piece lands and promotion
succeeds. Range-handler tests: (a) a range whose piece(s) are
already `_complete` is served directly with zero backend calls and zero
calls to `TriggerBackground`; (b) a range whose piece(s) are missing returns
the same "pending, retry" response as a cold `downloadBlob` miss, without
blocking the calling goroutine; (c) two concurrent range requests for
non-overlapping piece spans of the same digest each register their own
`TriggerBackground` slot (no artificial 202 on the second) and never
duplicate a backend fetch for any piece (`tryMarkDirty`, unaffected); a
concurrent range + whole-blob request for the same digest behaves the same
way — independent slots, no duplicate fetch for the overlapping piece; (d) a
range with `length > _maxRangeLength` gets 416, zero backend
calls, zero `TriggerBackground` calls; (e) the handler's readiness check goes
through `s.sched.ReadableRange`, not a fake internal piece snapshot —
`TestSchedulerReadableRangeWarm` (Complete() passthrough for agent/warm
torrents) and `TestSchedulerReadableRangePartial` (per-piece coverage: false
with any covering piece incomplete, true once all land) cover the new
`Scheduler.ReadableRange` method directly; (f) `TestSchedulerCopyReadyRangeSinglePiece`
/ `...MultiPieceBoundary` — offset/length within one piece and spanning a
piece boundary, exact byte match against a reference read;
`TestSchedulerCopyReadyRangeNotFound`; extended range-handler test asserts a
ready read calls `CopyReadyRange` and never `DownloadReader` (call-counting
fake).
**LOC (non-test):** ~125 (`cmd.go` wiring ~5, `TriggerBackground` ~9,
`downloadBlob` change ~12, new Range handler ~49, `ReadableRange` ~10,
`CopyReadyRange` ~25, `pieceRangeScope` ~10, `TorrentInfo.PieceLength` ~1,
call-site scope wiring ~4).

#### B6 — bounded concurrent piece prefetch

**Files:** `originstorage/torrent.go`; `lib/torrent/scheduler/constructors.go`
(thread a fetch-concurrency limit through to `NewPartialTorrent`);
`origin/cmd/config.go` + `config/origin/base.yaml` (the limit's config knob).
**Imports added:** none (channels + `sync` are already imported in
`torrent.go` for B3b).
**Declarations:**
```go
// originstorage/torrent.go
const _defaultFetchConcurrency = 8

// NewPartialTorrent gains a fetchConcurrency param (B3a's signature, extended):
func NewPartialTorrent(
    cas *store.CAStore, rd backend.RangeDownloader,
    namespace string, mi *core.MetaInfo, fetchConcurrency int) (*Torrent, error)
// body: unchanged from B3a, plus fetchSem: make(chan struct{}, fetchConcurrency).

func (t *Torrent) prefetchAhead(pi int)
// body: for i := pi + 1; i < pi+1+cap(t.fetchSem) && i < len(t.pieces); i++ {
//   if t.pieces[i].complete() { continue };
//   select { case t.fetchSem <- struct{}{}:
//     go func(i int) { defer func() { <-t.fetchSem }(); t.ensurePiece(i) }(i)
//   default: return /* pool full; retried on the next GetPieceReader call */ } }.
//   Errors from the background ensurePiece are not surfaced here — whichever
//   caller actually reads the piece calls GetPieceReader/ensurePiece itself
//   and gets the real error; this is a best-effort prefetch, not the read path.
```
`prefetchAhead` is called from `GetPieceReader` (B3b) right after the blocking
`ensurePiece(pi)` for the requested piece, so it runs on every path that reads
a partial origin torrent: B5's whole-blob drain, B5's Range-read handler, and
P2P's `handlePieceRequest` serving a peer. This replaces relying on the P2P
readahead signal (`streamReader.demand`, A7) — origin's `Torrent.HasPiece`
always returns `true` (B3a), so the `!HasPiece` branch that calls `demand`
never fires for origin, and A7's readahead window never reaches this code
path. `tryMarkDirty` (B3b) still elects exactly one fetcher per piece, so a
`prefetchAhead` background fetch racing a directly-blocking `GetPieceReader`
call for the same piece never double-fetches.
**Call-site edits:** `origin/cmd/cmd.go`/`config.go` — new
`scheduler.fetch_concurrency` (default `_defaultFetchConcurrency`), passed
through `NewOriginScheduler` → `TorrentArchive` → `NewPartialTorrent`.
**Tests:** `torrent_test.go` (extend) — a `GetPieceReader` call against a fake
slow `RangeDownloader` asserts up to `fetchConcurrency` concurrent
`DownloadRange` calls in flight and never more; a full `DownloadReader` drain
(B5's whole-blob path) completes in roughly `NumPieces/fetchConcurrency` fetch
rounds, not `NumPieces` serial rounds; `prefetchAhead` called from
overlapping windows (two `GetPieceReader` calls one piece apart) never
double-fetches a piece already dirty or complete; two goroutines completing
the last two pieces concurrently both call `MoveDownloadFileToCache` and the
blob still ends up correctly promoted exactly once (idempotent no-op on the
second call).
**LOC (non-test):** ~40 (`torrent.go` ~25, wiring ~15).

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
**Imports added:** `core/peer_info.go`: `github.com/willf/bitset` (see Verify
note below). `announce.go`/`announcer.go`: none.
**Declarations:**
```go
// core/peer_info.go — extend PeerInfo (after Complete bool):
type PeerInfo struct {
    PeerID   PeerID `json:"peer_id"`
    IP       string `json:"ip"`
    Port     int    `json:"port"`
    Origin   bool   `json:"origin"`
    Complete bool   `json:"complete"`
    // Bitfield is the peer's per-piece have-set, packed via bitset.BitSet's
    // own MarshalBinary — the same encoding lib/torrent/scheduler/conn's
    // agent-to-agent handshake already uses for bitfields (handshaker.go),
    // not a second, unpacked wire format alongside it. nil pre-V3.
    Bitfield []byte `json:"bitfield,omitempty"`
    // NumComplete is a cheap progress summary (== set-bit count; 0 if nil).
    NumComplete int `json:"num_complete,omitempty"`
}

// Variadic option keeps the ~12 existing 5-arg NewPeerInfo call sites compiling.
func NewPeerInfo(
    peerID PeerID, ip string, port int, origin, complete bool,
    opts ...PeerInfoOption) *PeerInfo // body: base struct + apply opts
type PeerInfoOption func(*PeerInfo)
func WithBitfield(b *bitset.BitSet) PeerInfoOption
// body: encoded, _ := b.MarshalBinary(); set Bitfield = encoded, NumComplete = int(b.Count()).
func PeerInfoFromContext(
    pctx PeerContext, complete bool, bitfield *bitset.BitSet) *PeerInfo

// tracker/announceclient/client.go
const V3 = 3
// Request needs no new top-level field — Peer *core.PeerInfo now carries the
// bitfield; V3 simply stops zeroing it. Response stays []*core.PeerInfo.
func getEndpoint(version int, addr string, h core.InfoHash) (method, url string)
// body: V1→GET /announce; V2→POST /announce/{h}; V3→POST /announce/v3/{h}.
func (c *client) Announce(
    d core.Digest, h core.InfoHash, complete bool, bitfield *bitset.BitSet,
    version int) ([]*core.PeerInfo, time.Duration, error)
// body: PeerInfoFromContext(c.pctx, complete, bitfield); for version<V3 nil out
// Peer.Bitfield before marshal (back-compat); else send it.
// D2 (below) extends this signature with a requested []int param — that's
// what to request, a separate concern from how progress is encoded here.

// tracker/trackerserver/announce.go — V3 handler mirrors V2; s.announce already
// passes the whole *core.PeerInfo to UpdatePeer + handout, so only req.Peer
// .Bitfield is newly populated.
func (s *Server) announceHandlerV3(w http.ResponseWriter, r *http.Request) error
```
**Call-site edits:**
- `trackerserver/server.go` `Handler()`: add
  `r.Post("/announce/v3/{infohash}", handler.Wrap(s.announceHandlerV3))` beside V2.
- `announcer/announcer.go`: call `Announce(d, h, complete, bitfield, ...,
  announceclient.V3)` with `bitfield` from the dispatcher's `torrent.Bitfield()`
  (already a `*bitset.BitSet`, no conversion needed — this is what made the
  raw-`[]bool` version pointless extra work); origins keep `Disabled()`.
  Extend `announceclient.DisabledClient.Announce` to match (still
  `ErrDisabled`). (D2 below fills in the `...` with `requested []int` — this
  call site is written once, not twice, since D1 is inert without D2 anyway.)
- `peerstore`: redis `serialize/deserializePeer` extend `pid:ip:port:complete`
  with the packed-bitfield bytes as a 5th field (omitted-empty so legacy
  4-field still parses); `local.peerEntry` gains `bitfield []byte`. **Verify**:
  redis set-member size grows with numPieces even packed — store the packed
  bytes or keep only `NumComplete` in redis and the full bitfield in `local`.
**Tests:** `func TestAnnounceV3CarriesBitfield` — V3 round-trips
`Bitfield`+`NumComplete` through `MarshalBinary`/`UnmarshalBinary`; V2 leaves
`Bitfield` nil; empty/nil; corrupt bytes rejected. `func TestPeerInfoWithBitfield`
— `NumComplete` == set-bit count; nil→0.
`func TestRedisStoreBitfieldRoundTrip` / `TestLocalStoreBitfieldRoundTrip` —
serialize parity, legacy 4-field still parses. `func TestGetEndpointV3`.
Regenerate `announceclient.Client` mocks (`make mocks`).
**LOC (non-test):** ~95.

#### D2 — handout policy preferring peers covering requested pieces

**Files:** `tracker/peerhandoutpolicy/peerhandoutpolicy.go` (thread requested
pieces into ranking); `peerhandoutpolicy/completeness_policy.go` (coverage-aware
variant); `peerhandoutpolicy/config.go` (register the policy name);
`trackerserver/announce.go` (`getPeerHandout` passes requested pieces);
`announceclient/client.go` (`Announce` gains `requested []int`, optional
`RequestedPieces []int` on `Request`); `dispatch/dispatcher.go` (producer:
`RequestedPieces()` accessor over A2's `demand`); `announcer/announcer.go`
(wires the two together).
**Imports added:** none (`core`, `sort`, `tally` present).
**Declarations:**
```go
// dispatch/dispatcher.go — producer. Reads A2's demand field under the same
// demandMu; this is the only place "pieces a streaming reader currently
// wants" is tracked, so D2 reuses it rather than adding a second tracker.
func (d *Dispatcher) RequestedPieces() []int
// body: lock; if !lazy || demand == nil { return nil }; collect set bit
// indices from d.demand via demand.NextSet/Visit; return.

// announceclient/client.go — optional V3-only field so a lazy peer advertises
// the pieces it currently wants.
type Request struct {
    Name            string         `json:"name"`
    Digest          *core.Digest   `json:"digest"`
    InfoHash        core.InfoHash  `json:"info_hash"`
    Peer            *core.PeerInfo `json:"peer"`
    RequestedPieces []int          `json:"requested_pieces,omitempty"` // V3
}

// Announce (declared in D1) gains a requested param here — conceptually a
// D2 concern (what to ask for), not D1's (how progress is encoded):
func (c *client) Announce(
    d core.Digest, h core.InfoHash, complete bool, bitfield *bitset.BitSet,
    requested []int, version int) ([]*core.PeerInfo, time.Duration, error)
// body: unchanged D1 PeerInfoFromContext construction, plus
// Request.RequestedPieces = requested for version>=V3 (nil out below V3,
// same back-compat rule D1 uses for Bitfield).

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
// "completeness" behavior's relative ordering and is opt-in).
const _coveragePolicy = "coverage"
type coverageAssignmentPolicy struct{}
func newCoverageAssignmentPolicy() assignmentPolicy
func (p *coverageAssignmentPolicy) assignPriority(
    source, peer *core.PeerInfo, requested []int) (int, string)
// body: origin→0 "origin"; complete seeder→1 "peer_seeder"; covers ≥1 requested
// piece→2 "peer_partial_covering"; else→3 "peer_incomplete". Origin checked
// before completeness — matches the existing completenessAssignmentPolicy
// (completeness_policy.go), which checks `peer.Origin` before `peer.Complete`;
// a peer that is both Origin and Complete must land in the origin bucket, not
// peer_seeder. (Verify: priority is
// the sole sort key today; to rank by *amount* covered, widen SortPeers to a
// (priority, -covered) tuple. Coarse buckets keep the int-priority contract.)

// Back-compat shims: defaultAssignmentPolicy + completenessAssignmentPolicy
// .assignPriority gain (source, requested) params and ignore them.
```
**Call-site edits:**
- `announcer/announcer.go`: periodic loop already reads `torrent.Bitfield()`
  for D1; add `requested := ctrl.dispatcher.RequestedPieces()` and pass it
  through: `Announce(d, h, complete, bitfield, requested, announceclient.V3)`.
  Eager dispatchers (never `SetLazy`) get `nil` back, same as today.
- `trackerserver/announce.go` `getPeerHandout`: `return s.policy.SortPeers(peer,
  peers, req.RequestedPieces), nil` — thread `requested []int` from
  `announceHandlerV3`; V1/V2 handlers pass `nil`.
- `peerhandoutpolicy/config.go` `NewPriorityPolicy` switch: add
  `case _coveragePolicy: p.policy = newCoverageAssignmentPolicy()`.
- `default_policy.go` + `completeness_policy.go`: update both `assignPriority`
  signatures to the 3-arg form (params ignored); update `fixtures.go` and all
  `SortPeers`/`assignPriority` call sites to the new arity.
**Tests:** `func TestDispatcherRequestedPieces` — lazy+demand set → matching
sorted slice; eager/nil demand → nil (mirrors A2's existing
`TestDispatcherLazyRequestsOnlyDemandedPieces` fixture). `func
TestCoveragePolicySortPeers` — complete seeder first regardless of
requested; partial peer covering requested ranked ahead of non-covering
incomplete; nil/empty requested degrades to completeness ordering (no regression);
nil peer bitfield ⇒ zero coverage; origin placement unchanged; source excluded;
a peer with both `Origin=true` and `Complete=true` lands in the origin bucket.
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

**Not to be confused with zstd:chunked** (§1) — an OCI image format already
supported today with no Kraken change, since Kraken's read path is
format-agnostic byte-range serving. This section is about zstd-compressing
Kraken's own on-disk pieces, an unrelated, separately-tracked workstream.

## 11. Testing strategy

- **Unit (each PR):** table-driven, `testify/require`. A1 tests priority
  reservation with fakes; A2/A3 test demand restriction + injection; A5/A7/A8 test
  the reader against a fake `storage.Torrent` that releases pieces on a schedule
  (sequential read, seek, range, lazy-demand window, EOF, terminal error); A11
  tests `DownloadBlobRange` against a fake HTTP server (happy path, repeated
  202-pending then success, terminal backoff timeout) mirroring the existing
  `DownloadBlob` retry tests, since it reuses the same `Poll` helper.
- **Scheduler integration:** extend existing scheduler tests so `DownloadReader`
  returns before completion and serves pieces as they land (eager after A6, lazy
  after A7).
- **Stack B:** B1 tests `DownloadRange` against each real backend's existing
  mock SDK fake (`gcsbackend`, `s3backend`) — first/interior/past-EOF-clamp,
  not-found → `backenderrors.ErrBlobNotFound`; B2 tests that `Exec` writes
  both the blob and the `/kmeta` sidecar (incl. the blob-already-exists backfill)
  and that the sidecar round-trips to the local `TorrentMeta`; B3 tests the
  `CAStore.MoveDownloadFileToCache` promotion (digest verify, atomic rename,
  double-promote no-op) and the partial `Torrent` against a real `*store.CAStore`
  (`DownloadDir` configured) fixture + a fake `RangeDownloader` (one
  `DownloadRange` per piece, sum-mismatch re-fetch, single-fetch under
  concurrency, restart durability, cache-hit short-circuit, completion
  promotes into cache); B3c tests piece keys never collide with digest keys
  and that `GetCacheFileReader`/`Stat`/`Metadata` never see a download-state
  or piece-cache-only digest as a hit; B4 tests `loadMetaInfo` cold (sidecar present ⇒
  partial torrent) and the whole-blob fallback when a backend lacks
  `RangeDownloader` or has no sidecar; B5 tests that a plain HTTP `downloadBlob`
  for a digest with an in-flight P2P partial fetch triggers a deduped
  background drain via `TriggerBackground`/`DownloadReader` (no duplicate
  backend download, no blocking of the request goroutine) that only
  completes once every piece has landed, plus the Range-read handler serving
  already-complete pieces directly with zero backend calls and returning the
  same non-blocking "pending, retry" response as `downloadBlob` on a miss;
  B6 tests `prefetchAhead` bounds in-flight `DownloadRange` calls to
  `fetchConcurrency` and that a full `DownloadReader` drain completes in
  fetch rounds rather than one piece at a time.
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
  **Format variant:** the harness is snapshotter-driven, not format-specific
  — pointing it at a `zstd:chunked`-converted image (`nerdctl image convert
  --zstdchunked`) exercises the identical read path, and is the variant to
  use for local validation wherever eStargz-specific build tooling
  (`ctr-remote image optimize`) isn't available.

## 12. Open questions

1. **Priority mechanism now classifies by blocked-piece vs. span-shape
   (A12).** `docs/lazy-pull-streaming-critical-review.md`'s finding that A1's
   flat, ascending-sorted `priority map[int]struct{}` had no per-stream or
   "currently blocked piece" classification is addressed: `Manager.priority`
   now carries a `PriorityClass` (Foreground/Background) per piece,
   upgrade-only, and `sortedPriority()` always serves the full Foreground
   group before the full Background group regardless of piece index — see
   §5.3 A12. The remaining, explicitly disclosed ceiling: priority claims are
   released on piece completion only, not on stream abandonment, so an
   aborted stream's claim can linger until the piece lands via some other
   path. Fixing that needs per-stream lifecycle tracking (and would also have
   to address the separately pre-existing, equally leaky `demand` bitset from
   A2) — real future work if traffic ever shows this matters, not scoped into
   A12.
2. **Stack B observability gap (per critical review).** Confirmed absent in
   code: no metrics for range-206 count, bytes fetched, duplicate-waiter
   count, fallback-to-full-refresh, or piece-vs-whole-blob `memCache`
   occupancy/eviction counts (B3c's cache reuse, §7.2, means the two
   populations share one pool with no breakdown today) anywhere in
   `originstorage/torrent.go`, `torrent_archive.go`, `lib/store/ca_store.go`,
   or `lib/backend/rangedownloader.go`. B5 closes the
   specific case of a plain HTTP whole-blob request racing an in-flight P2P
   partial fetch for the same digest (both now join the same
   `torrentControls`-deduped torrent); B6 bounds concurrent backend fetches
   per blob (`fetchSem`, sized by `scheduler.fetch_concurrency`). The fetch
   metrics above are still unbuilt. Required before Stack B is treated as
   fleet-ready; not required for the single-cluster devcluster PoC.
3. **`scheduler.fetch_concurrency` default (8) is unbenchmarked.** B6's
   `_defaultFetchConcurrency = 8` is a starting point, not a measured
   optimum — no Stack B code is built or profiled yet, so raising it (or
   making it adaptive to backend/network conditions) is real future tuning
   work once real traffic exists, not a correction to make blind now. It is
   already a config knob (`scheduler.fetch_concurrency`), so operators are
   not stuck with 8 in the meantime. **Acceptance criterion for revisiting
   the default** (per critical-review): once Stack B's observability (item 2
   above) ships, raise/adapt 8 only off a measured cold-origin benchmark —
   time-to-first-byte and backend saturation (error/throttle rate) at the
   default vs. a candidate value on the same corpus used in §11's e2e
   harness — not a guess; a config knob is not itself the acceptance bar.
4. **Cold-range 202-retry vs. true synchronous streaming.** A11/B5 reuse the
   existing whole-blob 202-retry pattern (`Poll`, already real on master) for
   cold range reads rather than holding the HTTP connection open and
   streaming bytes as the backend fetch progresses. A true streaming
   response would remove the retry round-trip's latency entirely, but it's a
   materially larger change (the origin handler would need to block/stream
   instead of returning immediately) that doesn't fit this stack's
   ≤150-line-PR constraint. Scoped as future work once Stack B's basic
   correctness and observability (item 2 above) are in place.
5. **A11's range backoff must stay inside the caller's own timeout, not
   Kraken's.** Verified against real snapshotter/containerd defaults:
   containerd's `[resolver] request_timeout_sec` defaults to 30s, and
   stargz-snapshotter's own per-chunk fetch timeout (`FetchTimeoutSec`)
   defaults to 300s — both far tighter than `defaultPollBackOff`'s 900s
   (15-minute) `MaxElapsedTime`. Reusing that whole-blob backoff for range
   reads (as originally drafted in A11) would mean Kraken keeps retrying long
   after the snapshotter has already given up and reissued its own request.
   Fixed in A11 (§5.2 above) with a dedicated `rangePollBackOff` (~20s
   `MaxElapsedTime`) sized under containerd's tighter default, with a note
   that deployments raising `request_timeout_sec` can raise this to match.
