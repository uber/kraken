# Kraken Performance Benchmark Harness

Workload-driven perf benchmark runner for Kraken. Implements the strategy in
[docs/BENCHMARKING.md](../../docs/BENCHMARKING.md). This first slice targets
the local devcluster; the K8s adapter and statsd-exporter integration will
follow.

## Quick start

Build images and install harness deps:

```
make images
pip install -r test/perf/requirements.txt
```

Run the smoke workload (smallest cold-pull, 1 rep):

```
make perf-bench-smoke
```

Run W1 (cold-pull-large-blob) with 5 reps and capture pprof:

```
python -m test.perf.runner \
  --env devcluster \
  --workload cold-pull-large-blob \
  --params blob_size=256MiB \
  --runs 5 \
  --profile \
  --baseline compare \
  --out bench-results/
```

Tear down:

```
python -m test.perf.runner --env devcluster --tear-down
```

## What's measured today

The first slice measures from the harness directly:

- end-to-end pull latency and throughput observed by the client
- end-to-end push latency and throughput observed by the client (when the
  workload uses uploads)
- pprof CPU, heap, mutex, and goroutine profiles per component (separate
  profile run, see strategy doc)

Per-component statsd metrics (e.g. `agent.download_throughput`,
`origin.replicate_blob`) are NOT scraped yet. The default devcluster does not
run a statsd_exporter and the configs do not point metrics anywhere. A
follow-up will spin up an exporter sidecar and overlay the dev configs.

## Resource constraints (constrained-capacity staging)

The devcluster start scripts now honor `KRAKEN_DOCKER_EXTRA_ARGS`. The
harness sets it per run:

```
KRAKEN_DOCKER_EXTRA_ARGS="--cpus=2 --memory=4g --memory-swap=4g"
```

Override with `--cpus` and `--memory` flags on the runner.

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
```

## Adding a workload

1. Add `test/perf/workloads/<name>.py` defining a `Workload` subclass.
2. Decorate it with `@register` from `test.perf.registry`.
3. Import it from `registry._bootstrap()`.
