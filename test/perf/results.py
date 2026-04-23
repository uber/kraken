"""Result writing, aggregation, and reporting for the perf harness."""
from __future__ import annotations

import datetime as dt
import json
import math
import os
import platform
import statistics
import subprocess
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable

from .workloads.base import KPISpec


@dataclass
class AggregatedKPI:
    name: str
    value: float
    unit: str
    direction: str
    tolerance_pct: float
    stat: str
    source: str
    samples: int
    tags: dict[str, str] = field(default_factory=dict)


def _percentile(values: list[float], p: float) -> float:
    """Linear-interpolated percentile, p in [0, 100]."""
    if not values:
        return float("nan")
    if len(values) == 1:
        return values[0]
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return s[int(k)]
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _apply_stat(values: list[float], stat: str) -> float:
    if not values:
        return float("nan")
    s = stat.lower()
    if s == "mean":
        return statistics.fmean(values)
    if s == "max":
        return max(values)
    if s == "min":
        return min(values)
    if s == "sum":
        return sum(values)
    if s.startswith("p") and s[1:].replace(".", "", 1).isdigit():
        return _percentile(values, float(s[1:]))
    raise ValueError(f"unknown statistic {stat!r}")


def aggregate_kpis(
    raw_runs: list[dict[str, Any]], specs: list[KPISpec]
) -> list[AggregatedKPI]:
    """Flatten a list of per-run raw dicts (workload.run() outputs) into
    aggregated KPIs. Only `source == 'harness'` is supported in the first
    slice; other sources will read from MetricsCollector.aggregate()."""
    aggregated: list[AggregatedKPI] = []
    for spec in specs:
        if spec.source != "harness":
            continue
        samples: list[float] = []
        for run in raw_runs:
            v = run.get(spec.query)
            if v is None:
                continue
            if isinstance(v, (list, tuple)):
                samples.extend(float(x) for x in v)
            else:
                samples.append(float(v))
        if not samples:
            continue
        value = _apply_stat(samples, spec.stat)
        aggregated.append(
            AggregatedKPI(
                name=spec.name,
                value=value,
                unit=spec.unit,
                direction=spec.direction,
                tolerance_pct=spec.tolerance_pct,
                stat=spec.stat,
                source=spec.source,
                samples=len(samples),
                tags=dict(spec.tags),
            )
        )
    return aggregated


def git_sha(repo_root: str) -> str:
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short=8", "HEAD"], cwd=repo_root
            )
            .decode()
            .strip()
        )
    except Exception:
        return "unknown"


def env_info(repo_root: str) -> dict[str, Any]:
    """Capture machine/cluster context that should accompany every run."""
    info: dict[str, Any] = {
        "git_sha": git_sha(repo_root),
        "captured_at": dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
        "kernel": platform.platform(),
        "python": platform.python_version(),
    }
    try:
        info["docker_version"] = (
            subprocess.check_output(["docker", "--version"], stderr=subprocess.DEVNULL)
            .decode()
            .strip()
        )
    except Exception:
        info["docker_version"] = "unknown"
    return info


@dataclass
class ResultsWriter:
    out_root: str
    env_name: str
    workload_name: str
    repo_root: str
    _seq: int = 0

    def new_run_dir(self) -> str:
        sha = git_sha(self.repo_root)
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        for seq in range(1000):
            run_id = f"{sha}_{ts}_{seq}"
            path = os.path.join(
                self.out_root, self.env_name, self.workload_name, run_id
            )
            if not os.path.exists(path):
                os.makedirs(path)
                self._symlink_latest(path)
                return path
        raise RuntimeError("too many runs in this second; bug?")

    def _symlink_latest(self, run_dir: str) -> None:
        latest = os.path.join(
            self.out_root, self.env_name, self.workload_name, "latest"
        )
        try:
            if os.path.islink(latest) or os.path.exists(latest):
                os.remove(latest)
            os.symlink(os.path.basename(run_dir), latest)
        except OSError:
            # Symlinks can fail in CI containers; non-fatal.
            pass

    def write_params(
        self, run_dir: str, params: dict[str, Any], tunables: dict[str, Any]
    ) -> None:
        path = os.path.join(run_dir, "params.json")
        with open(path, "w") as f:
            json.dump({"params": params, "tunables": tunables}, f, indent=2)

    def write_env(self, run_dir: str, info: dict[str, Any]) -> None:
        path = os.path.join(run_dir, "env.json")
        with open(path, "w") as f:
            json.dump(info, f, indent=2)

    def write_metrics(
        self,
        run_dir: str,
        aggregated: list[AggregatedKPI],
        raw_runs: list[dict[str, Any]],
    ) -> None:
        path = os.path.join(run_dir, "metrics.json")
        payload = {
            "kpis": [asdict(k) for k in aggregated],
            "raw_runs": raw_runs,
        }
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)

    def write_report(
        self,
        run_dir: str,
        aggregated: list[AggregatedKPI],
        comparison: list[dict[str, Any]] | None = None,
    ) -> None:
        lines = [
            f"# {self.workload_name} ({self.env_name})",
            "",
            "## KPIs",
            "",
            "| Metric | Stat | Value | Unit | Samples |",
            "| --- | --- | ---: | --- | ---: |",
        ]
        for k in aggregated:
            lines.append(
                f"| {k.name} | {k.stat} | {_fmt(k.value)} | {k.unit} | {k.samples} |"
            )
        if comparison:
            lines += ["", "## Baseline comparison", ""]
            lines += [
                "| Metric | Current | Baseline | Delta % | Verdict |",
                "| --- | ---: | ---: | ---: | --- |",
            ]
            for c in comparison:
                lines.append(
                    "| {name} | {cur} | {base} | {dlt} | {v} |".format(
                        name=c["name"],
                        cur=_fmt(c["current"]),
                        base=_fmt(c["baseline"]),
                        dlt=f"{c['delta_pct']:+.2f}%",
                        v=c["verdict"],
                    )
                )
        path = os.path.join(run_dir, "report.md")
        with open(path, "w") as f:
            f.write("\n".join(lines) + "\n")


def _fmt(x: float) -> str:
    if x != x:  # NaN
        return "nan"
    if abs(x) >= 1000:
        return f"{x:.1f}"
    if abs(x) >= 1:
        return f"{x:.3f}"
    return f"{x:.4g}"
