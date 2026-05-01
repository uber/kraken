# Kraken Performance Benchmark Harness

Workload-driven perf benchmark runner for Kraken. Implements the strategy
in [docs/BENCHMARKING.md](../../docs/BENCHMARKING.md). This first slice
targets the local devcluster; the K8s adapter and statsd-exporter
integration will follow.

## Prereqs

- Docker (Docker Desktop on Mac; Docker Desktop or vanilla Docker Engine,
  rootful or rootless, on Linux)
- Go 1.24+ on `PATH` (only needed once, for `make images`)
- Python 3.10+

## One-time setup

```bash
cd /home/user/kraken
git fetch origin
git checkout add-benchmarking-strategy

make images                                          # builds kraken-{agent,herd,...}:dev
pip install --break-system-packages -r test/perf/requirements.txt
```

`make images` cross-compiles Linux binaries inside a `golang:1.24.0`
container then builds the kraken docker images. Slow the first time.

`PYTHONPATH=.` is required when invoking `python -m test.perf.runner`
directly because Python's stdlib has a `test` package that shadows the
repo's. The Makefile targets set this for you.

## Smoke test (fastest sanity check)

```bash
make perf-bench-smoke
```

Brings up a fresh devcluster (1 herd + 2 agents), uploads a 16 MiB blob
to testfs, restarts the agents to flush per-agent cache, times one cold
pull from agent-1, writes results, leaves the cluster running.

Expected console (abridged):

```
Bringing up devcluster with 2 agents...
Cluster ready.
Run dir: /home/user/kraken/bench-results/devcluster/cold-pull-large-blob/<sha>_<ts>_0
Workload setup...
Workload warmup...
Run 1/1...
  -> {'download_seconds': 0.05, 'download_bytes': 16777216.0, 'download_throughput_mibps': 320.0}
Wrote /home/user/kraken/bench-results/...
```

## Look at what was written

```bash
ls bench-results/devcluster/cold-pull-large-blob/latest/
cat bench-results/devcluster/cold-pull-large-blob/latest/report.md
cat bench-results/devcluster/cold-pull-large-blob/latest/metrics.json
cat bench-results/devcluster/cold-pull-large-blob/latest/params.json
```

`report.md` is the human summary. `metrics.json` has aggregated KPIs and
per-run raw samples. `latest/` is a symlink to the most recent run dir.

## A real run (bigger blob, multiple reps)

```bash
PYTHONPATH=. python3 -m test.perf.runner \
  --env devcluster \
  --workload cold-pull-large-blob \
  --params blob_size=256MiB \
  --runs 5
```

If the cluster from the smoke step is still running, you don't need
`--bring-up`. Each run tears down and recreates the agent containers
(true cold leech cache); origin keeps its CAStore so it serves from
local disk after the first pull.

## Capture pprof

```bash
PYTHONPATH=. python3 -m test.perf.runner \
  --env devcluster \
  --workload cold-pull-large-blob \
  --params blob_size=256MiB \
  --runs 3 \
  --profile --cpu-seconds 30
```

Adds a profile-only pass that drives load while a 30 s CPU profile
records on every component. Build-index and proxy expose `/debug/pprof/*`
through nginx with stricter routing on devcluster (returns 404); the
collector skips those silently. Inspect what was captured:

```bash
go tool pprof bench-results/devcluster/cold-pull-large-blob/latest/profiles/origin/cpu.pb.gz
```

Then in pprof:

```
top10
```

## Baselines

First run captures a baseline:

```bash
PYTHONPATH=. python3 -m test.perf.runner \
  --env devcluster --workload cold-pull-large-blob \
  --params blob_size=256MiB --runs 5 \
  --baseline update
```

Writes `test/perf/baselines/devcluster/cold-pull-large-blob.json`.
Baselines are per host (capture them on the same machine you intend to
gate against), per env, per workload.

Subsequent runs compare:

```bash
PYTHONPATH=. python3 -m test.perf.runner \
  --env devcluster --workload cold-pull-large-blob \
  --params blob_size=256MiB --runs 5 \
  --baseline compare
```

Exit code 1 (and per-KPI table in stdout / `report.md`) on any KPI
breaching its tolerance.

## Resource constraints (constrained-capacity simulation)

Per-container limits when bringing up:

```bash
PYTHONPATH=. python3 -m test.perf.runner \
  --env devcluster --bring-up --agents 2 \
  --cpus 2 --memory 4g
```

The harness wires these into the devcluster start scripts via
`KRAKEN_DOCKER_EXTRA_ARGS`.

## More agents

```bash
PYTHONPATH=. python3 -m test.perf.runner --env devcluster --bring-up --agents 4
```

Spins agent-1 (16xxx ports), agent-2 (17xxx), agent-3 (18xxx),
agent-4 (19xxx) via `examples/devcluster/agent_n_start_container.sh`.
Pick which agent to pull from with `--params agent_idx=3`.

## Tear down

```bash
PYTHONPATH=. python3 -m test.perf.runner --env devcluster --tear-down
```

Removes all `kraken-*` containers. Equivalent to `make docker_stop`.

## How devcluster networking is wired

`examples/devcluster/_devcluster_lib.sh` (sourced by every start script)
creates a user-defined bridge network `kraken-bench` and joins every
container to it. The herd container is given a network alias
`host.docker.internal`, so the existing kraken configs keep working
unchanged. Cross-container traffic uses docker's embedded DNS; the
harness on the host still talks to containers via their published ports.

This setup works on Mac Docker Desktop, rootful Linux Docker, and
rootless Linux Docker. There is no longer a Docker Desktop requirement
for devcluster.

## What's measured today

The first slice measures from the harness directly:

- end-to-end pull latency and throughput observed by the client
- end-to-end push latency and throughput observed by the client (when a
  workload uses uploads)
- pprof CPU, heap, mutex, goroutine profiles per component (separate
  profile run, see the strategy doc)

Per-component statsd metrics (e.g. `agent.download_throughput`,
`origin.replicate_blob`) are NOT scraped yet. The default devcluster
does not run a statsd_exporter and the configs do not point metrics
anywhere. A follow-up will spin up an exporter sidecar and overlay the
dev configs.

## Output layout

```
bench-results/
  devcluster/
    <workload>/
      <git-sha>_<utc-iso>_<seq>/
        params.json
        metrics.json
        report.md
        profiles/<component>/{cpu,heap,mutex,goroutine}.pb.gz
        env.json
        raw/                 # per-scrape statsd snapshots (empty today)
      latest -> <run-id>     # symlink to most recent run
```

## Adding a workload

1. Add `test/perf/workloads/<name>.py` defining a `Workload` subclass.
2. Decorate it with `@register` from `test.perf.registry`.
3. Import it from `registry._bootstrap()`.

## Common issues

- **`cluster not ready after 120s`**: usually means images didn't build
  or a previous run left containers in a bad state. Re-run:

  ```bash
  make docker_stop
  make images
  docker logs kraken-herd --tail 200
  ```

- **`ModuleNotFoundError: test.perf`**: missing `PYTHONPATH=.`. The
  `make perf-bench` target sets it automatically.

- **Port already in use** (14000-19002): another devcluster or a stale
  container is bound. Run:

  ```bash
  make docker_stop
  ```
