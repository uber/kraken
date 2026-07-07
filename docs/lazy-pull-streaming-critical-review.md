• I reread the current docs. Several of my earlier findings are now fixed, so the review changes materially.

What is fixed now:

- Cold Stat is no longer implemented via DownloadReader; it uses a metainfo-only Scheduler.Stat, which is the right correction. docs/lazy-pull-streaming-
  production-plan.md:513

- The blobserver no longer reaches into partial-torrent internals directly; it uses explicit ReadableRange and CopyReadyRange APIs. docs/lazy-pull-streaming-
  production-plan.md:1451

- The server-side range trigger path no longer allocates make([]byte, length); it drains via io.NewSectionReader and caps request size at 64 MiB. docs/lazy-
  pull-streaming-production-plan.md:1425 docs/lazy-pull-streaming-production-plan.md:1474

- Trigger dedupe is no longer digest-only; disjoint ranges can proceed independently. That closes one real source of artificial serialization under snapshotter
  traffic. docs/lazy-pull-streaming-production-plan.md:1385 docs/lazy-pull-streaming-production-plan.md:1542

- The tracker section is also in better shape now: the requested-piece producer path is defined, and the raw []bool issue appears to have been replaced by
  packed bitfield bytes. docs/lazy-pull-streaming-production-plan.md:1851 docs/lazy-pull-streaming-production-plan.md:1872

Remaining findings

1. The biggest remaining product gap is still the cold-range 202 retry model. The docs now explicitly acknowledge this as future work, but it is still the main
   reason Kraken’s cold-origin lazy path will lag a more mature system like Dragonfly on first-touch latency. Every cold range miss still pays at least one
   extra request cycle instead of turning into a true live stream. docs/lazy-pull-streaming-production-plan.md:2057

2. The priority model is still not production-ready for multiple concurrent streams. The doc is explicit that per-stream classification is missing and still
   future work. I still consider this a high-severity issue for fleet rollout, because once multiple lazy layers or mixed Read/ReadAt traffic coexist,
   “priority as one ascending-sorted map” is too weak. docs/lazy-pull-streaming-production-plan.md:2027

3. The client-side range retry path still has an unbounded request-sized allocation. clusterClient.DownloadBlobRange still builds bytes.NewBuffer(make([]byte,
   0, length)) before polling. The server cap helps only after the request arrives; the client allocates first. That is still a memory-risk bug for large or
   malformed requests. docs/lazy-pull-streaming-production-plan.md:625

4. Stack B observability is still below production bar, and the doc correctly says so. The missing metrics are exactly the ones you will need to understand why
   cold lazy pulls are slow or memory-heavy: range-206 counts, bytes fetched, duplicate waiters, fallback-to-full-refresh, and shared memcache occupancy/
   evictions. docs/lazy-pull-streaming-production-plan.md:2037

5. fetch_concurrency=8 is now properly called out as unbenchmarked and configurable, which is better than before, but it still means the current plan does not
   yet explain how Kraken will avoid under-driving cold backends on first-touch reads. This is no longer a design bug; it is now a tuning risk. docs/lazy-pull-
   streaming-production-plan.md:2050

6. The one-store invariant is now much stronger and has the right negative tests: cache readers/stats/metadata must miss while only partial data exists in
   download state. That concern is mostly closed. The remaining caution is operational rather than conceptual: once B3c’s piece-level memcache reuse is added,
   you will need the missing observability from item 4 to prove partial-piece cache pressure is not evicting warm whole blobs too aggressively. docs/lazy-pull-
   streaming-production-plan.md:1184 docs/lazy-pull-streaming-production-plan.md:2039

Bottom line

The plan is materially better now. The earlier contract/API problems are mostly fixed. What remains is less “the design is wrong” and more “the current phase
still chooses a latency-taxed cold-path and postpones the scheduling/observability work needed for fleet-grade performance.”

If you want the shortest updated judgment: the current plan is now structurally sound enough to build, but it will still likely trail Dragonfly on cold-cache
lazy startup until you address:

- true synchronous cold-range streaming,
- stream-aware priority,
- better Stack B observability,
- and higher or adaptive cold-origin concurrency.