# Phase 1 streaming PoC — TTFB before/after (make devcluster)

Setup: kraken devcluster (origin warm, agents cold). Baseline = agent-one
blocking path (`GET .../blobs/<d>`); Stream = agent-two `?stream=1` (serves
pieces in order as they arrive). testfs backend, random blobs, in_order piece
policy. Integrity (sha256) verified equal to the source blob for every pull.

| blob size | baseline TTFB | stream TTFB | TTFB speedup | baseline total | stream total |
|-----------|---------------|-------------|--------------|----------------|--------------|
| 128 MB    | 0.115 s       | 0.019 s     | 5.9x         | 0.199 s        | 0.147 s      |
| 256 MB    | 0.204 s       | 0.019 s     | 11.0x        | 0.365 s        | 0.284 s      |
| 512 MB    | 0.723 s       | 0.019 s     | 38.9x        | 1.024 s        | 0.748 s      |
| 1024 MB   | 2.428 s       | 0.018 s     | 136x         | 3.098 s        | 2.455 s      |

Key observations:
- Stream TTFB is ~constant (~18-19 ms = time to the first piece), independent of
  blob size — exactly the streaming property. Baseline TTFB grows linearly with
  blob size because it must download the whole blob before serving a byte.
- Total transfer time is also lower for streaming (serving overlaps with the
  download tail), with no integrity regression.

Reproduce: `bash bench-results/stream_ttfb_bench.sh` against a running
`make devcluster` (on Linux, start containers with
`--add-host host.docker.internal:172.17.0.1`).
