# Kraken Holistic Benchmarking Strategy (Staging / Constrained Capacity)

## Context

Kraken today ships exactly one Go micro-benchmark
(`lib/backend/sqlbackend/benchmark/client_test.go`) and ad-hoc Python
integration tests. There is no continuously-measured, reproducible performance
signal for the P2P pull, push, or tag-replication hot paths. Recent commits
(e.g. 396c1a1e, db728c91, 996feef4) added a strong set of throughput/latency
metrics; those are now the foundation we can build a benchmark suite on.

This document specifies a workload-driven benchmark suite that runs in **both**
devcluster (fast iteration) and a **K8s staging cluster** (real signal),
produces stored baselines, gates regressions in CI, and surfaces saturation
curves for capacity planning. Scope is steady-state perf only (no chaos).

## 1. Workload Catalog

Each workload lives at `/test/perf/workloads/<name>.py` as a class implementing
`Workload` (Section 3). KPIs reference metrics already emitted today.

| ID | Name | Hot path / tunables | Inputs | Termination | KPIs (statistic) |
|----|------|---------------------|--------|-------------|------------------|
| W1 | `cold-pull-large-blob` | Cold pull, origin seed; `origin_pipeline_limit`, `piece_request_*` | 1 blob in {256 MiB, 1 GiB, 4 GiB}; 1 agent; cold cache; 5 reps | All pulls complete | `agent.download_time` p50/p95/p99 by `size_bucket`; `agent.download_throughput` p50/p95; `origin.replicate_blob` p95; origin HTTP `latency` p99 on `/blobs/*`; CPU s/GB at origin |
| W2 | `concurrent-pull-fanout` | Warm-cache P2P pull; `pipeline_limit`, `max_open_conn`, `bandwidth.*` | 1 blob {512 MiB}; agents in {2, 4, 8, 16} (devcluster: 2-4); start within 1s | All agents finish | per-agent `download_throughput` p50/p95; max/min skew; `tracker./announce` p99; `origin.bytes_downloaded` total |
| W3 | `cold-pull-many-small-blobs` | Manifest + many small fetches; `num_incoming_workers`, `peer_handout_limit` | 200 layers x 1 MiB; 1 agent; cold | All complete | `agent.metainfo_download_time` p99; `agent.download_time` p99 (1 MiB bucket); tracker peer handout latency p99 |
| W4 | `upload-throughput` | Push: proxy `startClusterUploadHandler` then origin `uploader.go`; testfs/S3 | Concurrent pushers in {1, 4, 16}; sizes {16 MiB, 256 MiB, 1 GiB}; 60s steady state | Time-bounded | proxy `blob_upload` rate; `bytes_uploaded` MiB/s; origin start/patch/commit p95/p99; `origin.replicate_blob` p95 |
| W5 | `tag-replication-burst` | Build-index tagserver then persistedretry; `num_incoming_workers`, `num_retry_workers` | 500 distinct tags / 30s; 64 MiB blobs; 2 build-index zones | Replication queue drains | end-to-end tag visible at remote: p50/p95/p99 lag; `/internal/tags` p99; persistedretry task throughput |
| W6 | `mixed-r-w` | Push and pull simultaneously, cache pressure | 4 pushers (256 MiB), 8 pullers, 5 min steady state | Time-bounded | proxy upload p95; agent download p95; origin CPU; `replicate_blob_errors` rate |
| W7 | `endgame-stress` | Last-piece selection; `piece_request_policy` (`default` vs `rarest_first`) | 1 blob {1 GiB}; 8 agents (K8s) staggered every 2s | All complete | last-piece tail p99; piece-retransmit count; per-agent throughput variance |
| W8 | `scaling-agents` | Saturation curve: vary agent count | 1 blob {512 MiB}; agents in {1, 2, 4, 8, 16, 32, 64} (K8s) | All complete | `agent.download_throughput` p50 vs N; `tracker.announce` p99 vs N; origin egress MiB/s |
| W9 | `tunable-sweep` | Meta: re-runs W2/others across grid (Section 6) | See sweep grid | Each cell complete | KPIs from base workload per cell |

Coverage: cold pull (W1, W3), warm/P2P (W2, W7, W8), upload (W4),
tag replication (W5), mixed (W6), scaling (W8), config sweep (W9).

## 2. Metric & Profile Collection

### Metrics (statsd_exporter then Prometheus text)

Per-component scrape URLs (devcluster):

- proxy `:15000/metrics`, origin `:15002`, tracker `:15003`,
  build-index `:15004`, agent-1 `:16002`, agent-2 `:17002`
- K8s: `kubectl port-forward` per pod, harness assigns local ports.

Cadence: continuous scrape at 1s during the workload + a final flush scrape
10s after workload ends (statsd_exporter default flush is 1s).

Snapshot: each scrape is stored as `metrics-<unix-ts>.prom`; aggregated to
`metrics.json` with derived stats (p50/p95/p99 from histograms via linear
interpolation of `_bucket{le=...,size_bucket=...}` series; rates from
counters).

### pprof (separate "profile run" to avoid measurement perturbation)

For each workload, run twice: a "measure" run (no profiling), then a "profile"
run with identical inputs collecting:

```
curl -s "http://<host>:<port>/debug/pprof/profile?seconds=30" -o cpu.pb.gz   # 5s into steady state
curl -s "http://<host>:<port>/debug/pprof/heap"      -o heap.pb.gz           # at end
curl -s "http://<host>:<port>/debug/pprof/mutex"     -o mutex.pb.gz          # at end (mutex_profile_fraction=1 in devcluster already)
curl -s "http://<host>:<port>/debug/pprof/goroutine" -o goroutine.pb.gz      # at end
```

pprof port is the same admin port as metrics on Kraken components.

### Artifact layout

```
bench-results/
  <env>/                              # devcluster | k8s-staging
    <workload>/
      <run-id>/                       # <git-sha>_<utc-iso>_<seq>
        params.json                   # workload inputs + tunable overrides
        metrics.json                  # aggregated KPIs
        report.md                     # human-readable summary used by PR comment
        raw/metrics-<ts>.prom         # one per scrape
        profiles/<component>/{cpu,heap,mutex,goroutine}.pb.gz
        logs/<component>.log
        env.json                      # versions, kernel, cgroup limits
```

## 3. Test Harness Design

Language: **Python**, building on `/test/python/components.py` (which already
provides `Cluster`, `OriginCluster`, `Agent`, `Proxy`, `BuildIndex`, `Tracker`,
`TestFS`). Reuse `/test/python/uploader.py` (synthetic pushes) and
`/test/python/utils.py:concurrently_apply`.

### Directory layout (all CREATE)

```
/test/perf/
  __init__.py
  runner.py                 # Click CLI orchestrator (~250 LOC)
  registry.py               # workload name -> class
  collect.py                # MetricsCollector, ProfileCollector
  results.py                # ResultsWriter, compare(), report.md writer
  baseline.py               # load/save/compare baselines
  envs/
    __init__.py
    devcluster.py
    k8s.py
    k8s-staging.values.yaml # helm overrides for staging
  workloads/
    __init__.py
    base.py                 # Workload base class
    cold_pull.py            # W1, W3
    fanout.py               # W2
    upload.py               # W4
    tag_replication.py      # W5
    mixed.py                # W6
    endgame.py              # W7
    scaling.py              # W8
    sweep.py                # W9
  templates/                # Jinja2 templates seeded from current configs
    {agent,origin,tracker,build-index,proxy}.yaml.j2
  baselines/
    devcluster/<workload>.json
    k8s-staging/<workload>.json
  requirements.txt          # click, requests, prometheus-client, numpy, jinja2, matplotlib (opt)
  README.md
```

### Class shape

`workloads/base.py`:

```python
class Workload:
    name: str
    default_params: dict
    def setup(self, env, params): ...           # build images, scale, push tunables
    def generate_data(self, env, params): ...   # synthetic blobs via uploader.py
    def warmup(self, env, params): ...
    def run(self, env, params) -> dict: ...     # returns timing summary
    def kpis(self) -> list[KPISpec]: ...        # metric name + statistic + tag filter
    def teardown(self, env): ...
```

`runner.py` CLI:

```
python -m test.perf.runner \
  --workload <name> --env devcluster|k8s-staging \
  --runs 5 --params blob_size=512MiB,agents=4 \
  --tunables pipeline_limit=5,piece_request_policy=rarest_first \
  --baseline compare|update|skip \
  --profile  --out bench-results/
```

`collect.py`:

- `MetricsCollector(env)`: `start()`, `stop()`, `scrape_once()`,
  `aggregate(workload.kpis())`. Reuses `_get_metric_value` style from
  `test/python/test_memory_cache.py`.
- `ProfileCollector(env)`: `capture(component, kinds, duration)`.

`results.py`:

- `ResultsWriter.write(run_id, params, kpis, raw, profiles)` produces the
  directory tree above.
- `compare(current, baseline) -> Report` with per-KPI delta and pass/fail.

### Synthetic blobs

Reuse `/test/python/uploader.py`. Generate with `os.urandom` seeded from
`(workload, run_idx, blob_idx)` for reproducibility. Tag as
`bench/<workload>/<idx>:<size>`. Note: synthetic random data has no dedup
(conservative bandwidth, lower-bound hit rates). Documented in README.

### Parameterizing topology

- **Devcluster**: `envs/devcluster.py` re-executes
  `examples/devcluster/agent_*_start_container.sh` style scripts via
  subprocess; replaces them with a single parameterized
  `agent_start_container.sh` taking `$AGENT_INDEX` (Section 9 file changes).
- **K8s**: `kubectl scale deployment/<comp> --replicas=N` (or DaemonSet via
  node selectors). Origin/tracker/build-index counts via
  `helm upgrade --set <comp>.replicas=N`.

### Setting tunables per run

- **Devcluster**: harness rewrites
  `examples/devcluster/config/<comp>/development.yaml` from Jinja2 templates
  in `/test/perf/templates/` before `make devcluster`. Records overridden keys.
- **K8s**: `helm upgrade --set scheduler.dispatch.pipelineLimit=5 --set scheduler.dispatch.pieceRequestPolicy=rarest_first`.
  Requires templating these into `/helm/config/<comp>.yaml` (Section 9).

## 4. Devcluster Execution Path

### Pre-flight (one time)

```
make install-hooks
make images
docker pull prom/statsd-exporter
pip install -r test/perf/requirements.txt
```

### Resource constraints (REQUIRED for repeatability)

Devcluster scripts run unconstrained today. Harness wraps each `docker run`:

```
docker run --cpus=2 --memory=4g --memory-swap=4g \
           --network host --name <component> ... <image>
```

Implemented in `envs/devcluster.py` by setting
`KRAKEN_DOCKER_EXTRA_ARGS="--cpus=2 --memory=4g"` and modifying the start
scripts to honor it (Section 9).

### Bring-up + scale

```
python -m test.perf.runner --env devcluster --bring-up --agents 4
```

Runs `make devcluster`, then launches `agent-3`, `agent-4` via templated
scripts.

### Per-workload command

```
python -m test.perf.runner \
  --env devcluster \
  --workload cold-pull-large-blob \
  --params blob_size=256MiB \
  --runs 5 \
  --baseline compare \
  --out bench-results/
```

Output: `bench-results/devcluster/cold-pull-large-blob/<run-id>/...`. Exit 0
if all KPIs within tolerance; exit 1 otherwise.

### Tear-down

```
python -m test.perf.runner --env devcluster --tear-down
```

## 5. K8s Staging Execution Path

### Cluster shape (recommended baseline, "constrained capacity")

| Component | Replicas | CPU req/lim | Mem req/lim | Notes |
|-----------|----------|-------------|-------------|-------|
| origin | 3 | 500m / 1000m | 512Mi / 1Gi | hashring intact |
| tracker | 3 | 250m / 500m | 256Mi / 512Mi | |
| build-index | 3 | 250m / 500m | 256Mi / 512Mi | |
| proxy | 2 | 500m / 1000m | 256Mi / 512Mi | |
| agent (DaemonSet) | per-node, 4-16 nodes | 500m / 1000m | 512Mi / 1Gi | |
| testfs | 1 | 500m / 1000m | 1Gi / 2Gi | baseline backend |

Intentionally small to surface saturation in W8.

### Helm changes (Section 9 file list)

- Add `resources:` blocks per component in `/helm/values.yaml`.
- Add PDBs in `/helm/templates/<comp>.yaml`.
- Add `topologySpreadConstraints` for `agent` and `origin`.
- Expose pprof + metrics ports in Service (or rely on `kubectl port-forward`).

### Harness driving K8s (`envs/k8s.py`)

- `bring_up()`: `helm upgrade --install kraken /helm --values /test/perf/envs/k8s-staging.values.yaml --set <tunables>`
- `scale(component, n)`: `kubectl scale ...`
- `metrics_endpoints()` and `pprof_endpoints()`: `kubectl get pods -l app=<c>`,
  then per-pod `kubectl port-forward`; harness assigns local ports.
- `teardown()`: `helm uninstall kraken`.

### Per-workload command

```
python -m test.perf.runner --env k8s-staging --workload scaling-agents \
  --params blob_size=512MiB,agents='[1,2,4,8,16]' --runs 3 --baseline compare
```

## 6. Tunable Sweep Methodology

| Tunable | Path | Sweep | Most-sensitive workload |
|---------|------|-------|-------------------------|
| `pipeline_limit` | `lib/torrent/scheduler/dispatch/config.go` | 1, 2, 3, 5, 8 | W2 |
| `origin_pipeline_limit` | same | 1, 3, 5, 8, 12 | W1 |
| `piece_request_policy` | same | `default`, `rarest_first` | W7 |
| `bandwidth.{egress,ingress}_bits_per_sec` | `lib/torrent/scheduler/conn/config.go` | 100M, 200M, 500M, 1G | W2, W8 |
| `max_open_conn` | `lib/torrent/scheduler/connstate/config.go` | 5, 10, 20 | W2 |
| `announce_interval` | `tracker/trackerserver/config.go` | 1s, 3s, 10s | W2, W8 |
| `peer_handout_limit` | same | 10, 50, 100 | W3, W8 |
| `num_incoming_workers` | `lib/persistedretry/config.go` | 2, 4, 8 | W5 |

### Encoding

`workloads/sweep.py:SweepWorkload(base_workload, axes)` produces a Cartesian
(or named subset) of cells. Results live under
`bench-results/<env>/sweep-<base>/<axis1>=<v>__<axis2>=<v>/<run-id>/`.
`--reduce` flag enables Latin-square instead of full grid to bound wall time.

### Comparison

```
python -m test.perf.results plot --sweep <dir> \
  --kpi agent.download_throughput.p95 \
  --x pipeline_limit --series origin_pipeline_limit
```

Outputs `plot.png` (matplotlib) + `summary.csv`.

## 7. Baseline Storage & Regression Detection

### Format `/test/perf/baselines/<env>/<workload>.json`

```json
{
  "workload": "cold-pull-large-blob",
  "env": "devcluster",
  "git_sha": "<abbrev>",
  "captured_at": "2026-04-23T12:00:00Z",
  "params": {"blob_size": "256MiB"},
  "kpis": [
    {"metric": "agent.download_throughput", "stat": "p50",
     "tags": {"size_bucket": "256MiB"},
     "value": 215.0, "unit": "MiB/s",
     "tolerance_pct": 10.0, "direction": "higher_better"},
    {"metric": "agent.download_time", "stat": "p99",
     "tags": {"size_bucket": "256MiB"},
     "value": 5.2, "unit": "s",
     "tolerance_pct": 15.0, "direction": "lower_better"}
  ]
}
```

### Comparison rule

For each KPI, compute `delta = (current - baseline) / baseline`. Fail if:

- `direction=lower_better and delta > tolerance_pct/100`, or
- `direction=higher_better and delta < -tolerance_pct/100`.

### Update process

- `--baseline update` writes the new file.
- CI rejects PRs touching `/test/perf/baselines/**` unless they also touch
  source code AND a CODEOWNERS-listed perf reviewer approves.
- Baselines split per env (devcluster vs k8s-staging) and per release
  (`/test/perf/baselines/<env>/v<release>/<workload>.json`); main writes to
  `unreleased/`; release tagging promotes them.

## 8. CI Integration

CREATE `/.github/workflows/perf-bench.yml`:

```yaml
on:
  schedule: [{cron: "23 7 * * *"}]
  workflow_dispatch: {inputs: {workloads: {default: "all"}}}
  pull_request:
    types: [labeled, synchronize]

jobs:
  devcluster:
    if: github.event_name != 'pull_request' || contains(github.event.pull_request.labels.*.name, 'perf')
    runs-on: ubuntu-latest
    strategy:
      matrix:
        workload: [cold-pull-large-blob, concurrent-pull-fanout,
                   cold-pull-many-small-blobs, upload-throughput,
                   tag-replication-burst, mixed-r-w]
      fail-fast: false
    concurrency: {group: "perf-${{ matrix.workload }}", cancel-in-progress: false}
    steps:
      - uses: actions/checkout@v4
      - run: make images
      - run: pip install -r test/perf/requirements.txt
      - run: python -m test.perf.runner --env devcluster
                 --workload ${{ matrix.workload }} --runs 3
                 --baseline compare --out bench-results/
      - uses: actions/upload-artifact@v4
        with: {name: "perf-${{ matrix.workload }}", path: bench-results/, retention-days: 30}
      - uses: marocchino/sticky-pull-request-comment@v2
        if: github.event_name == 'pull_request'
        with: {header: "perf-${{ matrix.workload }}",
               path: bench-results/devcluster/${{ matrix.workload }}/latest/report.md}

  k8s-staging:
    if: github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'
    runs-on: [self-hosted, k8s-staging]    # NOTE: requires self-hosted runner with kubeconfig
    strategy: {matrix: {workload: [scaling-agents, endgame-stress, tag-replication-burst]}}
    steps: ...same shape, --env k8s-staging
```

Notes:

- `cancel-in-progress: false` so we never abort an in-flight measurement.
- W9 (sweep) is too long for nightly: separate weekly cron `0 6 * * 0`.
- PR comment aggregates `current vs baseline (delta%)` per KPI.

## 9. File-Level Changes (Exhaustive)

### Harness (Section 3)

- CREATE `/test/perf/__init__.py`
- CREATE `/test/perf/runner.py`
- CREATE `/test/perf/registry.py`
- CREATE `/test/perf/collect.py`
- CREATE `/test/perf/results.py`
- CREATE `/test/perf/baseline.py`
- CREATE `/test/perf/envs/__init__.py`
- CREATE `/test/perf/envs/devcluster.py`
- CREATE `/test/perf/envs/k8s.py`
- CREATE `/test/perf/envs/k8s-staging.values.yaml`
- CREATE `/test/perf/workloads/{base,cold_pull,fanout,upload,tag_replication,mixed,endgame,scaling,sweep}.py`
- CREATE `/test/perf/templates/{agent,origin,tracker,build-index,proxy}.yaml.j2`
  (seeded from current configs)
- CREATE `/test/perf/baselines/{devcluster,k8s-staging}/<workload>.json`
  (populated after first green run)
- CREATE `/test/perf/requirements.txt`
- CREATE `/test/perf/README.md`

### Devcluster (Section 4)

- MODIFY `/Makefile`: add `perf-bench` and `perf-bench-smoke` targets.
- MODIFY `/examples/devcluster/{agent_one,agent_two,herd}_start_container.sh`:
  honor `KRAKEN_DOCKER_EXTRA_ARGS` so the harness can inject `--cpus`/
  `--memory`. Replace `agent_one`/`agent_two` with a parameterized
  `agent_start_container.sh` taking `$AGENT_INDEX`.
- MODIFY `/examples/devcluster/config/{agent,origin,tracker,build-index,proxy}/development.yaml`:
  ensure all sweep keys are surfaced (some currently rely on Go defaults). The
  harness substitutes via templates seeded from these files.

### K8s / Helm (Section 5)

- MODIFY `/helm/values.yaml`: add for each component:

  ```yaml
  origin:
    replicas: 3
    resources:
      requests: {cpu: 500m, memory: 512Mi}
      limits:   {cpu: 1000m, memory: 1Gi}
    pdb: {minAvailable: 2}
  tracker:    {replicas: 3, resources: {...}, pdb: {minAvailable: 2}}
  buildIndex: {replicas: 3, resources: {...}, pdb: {minAvailable: 2}}
  proxy:      {replicas: 2, resources: {...}}
  agent:      {resources: {...}}
  testfs:     {resources: {...}}
  metrics:    {scrape: true, port: 5005}
  ```

- MODIFY `/helm/templates/{origins,trackers,build-index,proxy,agents,testfs}.yaml`:
  reference `.Values.<comp>.resources` and `.Values.<comp>.pdb`; gate a
  `PodDisruptionBudget` resource; add `topologySpreadConstraints` for `origin`
  and `agent`; expose pprof + metrics ports.
- MODIFY `/helm/config/{agent,origin,tracker,build-index,proxy}.yaml`:
  templatize sweep tunables so `helm --set scheduler.dispatch.pipelineLimit=5`
  works (`{{ .Values.scheduler.dispatch.pipelineLimit | default 3 }}` etc.).

### CI (Section 8)

- CREATE `/.github/workflows/perf-bench.yml`.
- MODIFY (or CREATE) `/.github/labeler.yml`: add `perf` label rule for changes
  under `/test/perf/**` and known hot-path files (`lib/torrent/**`,
  `tracker/**`, `origin/**`, `proxy/**`, `agent/**`, `build-index/**`,
  `lib/hashring/**`, `lib/persistedretry/**`).
- MODIFY (or CREATE) `/.github/CODEOWNERS`: entry for `/test/perf/baselines/`.

## 10. Verification

1. **Smoke**: `make perf-bench-smoke` runs the smallest workload end-to-end.
   Expect `bench-results/devcluster/cold-pull-large-blob/<run-id>/{params,metrics,env}.json`.
2. **Sanity**: `python -m test.perf.results validate <run-dir>` ensures every
   KPI value > 0 and `bytes_downloaded` > expected blob size (catches empty
   statsd_exporter scrapes).
3. **pprof load test**:
   `go tool pprof bench-results/.../profiles/origin/cpu.pb.gz`,
   `(pprof) top10` returns non-empty.
4. **Baseline regression test**: run twice without code changes; the second
   run with `--baseline compare` PASSES within tolerance for at least 9 of
   10 reps.
5. **Workflow dry run**: `act -W .github/workflows/perf-bench.yml -j devcluster`
   locally, or `gh workflow run perf-bench.yml`.
6. **Sweep correctness**:
   `--workload tunable-sweep --axes pipeline_limit=1,3,8 --base concurrent-pull-fanout`
   produces a results tree + `summary.csv` with the correct cell count.
7. **K8s shake-out**:
   `python -m test.perf.runner --env k8s-staging --bring-up --kubecontext staging`
   then run W1 with the smallest blob; confirm port-forward + scrape works
   for all pods.

## 11. Open Questions / Risks

- **Profile perturbation**: CPU/mutex profiling alters timing. Mitigated by
  separate measure vs profile runs (Section 2). Profile-run KPIs are advisory
  only, never compared to baselines.
- **statsd_exporter flush timing**: 1s default; the 10s post-run scrape is
  conservative but tail counters may still be in-flight. Document; consider
  switching long-running components to direct Prometheus client emission later.
- **Devcluster vs K8s divergence**: different hardware, no host-network in
  K8s, container runtime overhead. Keep two baseline trees; never compare
  across envs. Devcluster strictly for same-env regression detection; K8s
  for absolute numbers and capacity planning.
- **Self-hosted runner**: K8s jobs need a runner with `kubectl` + kubeconfig
  + reach to staging. Recommend a single self-hosted runner with
  `concurrency=1` to avoid clobbering the cluster. Document setup in
  `/test/perf/README.md`.
- **Synthetic vs real images**: workloads use random blobs (no dedup). Real
  images compress. Numbers are conservative bandwidth upper bounds; flagged
  in README.
- **Cold cache enforcement**: "Cold" requires flushing origin + agent caches
  between runs. Devcluster: harness `docker stop`/`start` containers between
  runs. K8s: `kubectl rollout restart` (slower); `--cold` default for W1/W3.
- **CI minute budget**: Full sweep (W9) too long for nightly; weekly cron only.
