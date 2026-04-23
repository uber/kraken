"""Baseline storage and regression comparison."""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from typing import Any

from .results import AggregatedKPI


@dataclass
class BaselineKPI:
    name: str
    stat: str
    value: float
    unit: str
    direction: str
    tolerance_pct: float
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class Baseline:
    workload: str
    env: str
    git_sha: str
    captured_at: str
    params: dict[str, Any]
    kpis: list[BaselineKPI]


def baseline_path(baselines_root: str, env: str, workload: str) -> str:
    return os.path.join(baselines_root, env, f"{workload}.json")


def load(path: str) -> Baseline | None:
    if not os.path.exists(path):
        return None
    with open(path) as f:
        data = json.load(f)
    kpis = [BaselineKPI(**k) for k in data.get("kpis", [])]
    return Baseline(
        workload=data["workload"],
        env=data["env"],
        git_sha=data.get("git_sha", "unknown"),
        captured_at=data.get("captured_at", ""),
        params=data.get("params", {}),
        kpis=kpis,
    )


def save(
    path: str,
    workload: str,
    env: str,
    git_sha: str,
    captured_at: str,
    params: dict[str, Any],
    aggregated: list[AggregatedKPI],
) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {
        "workload": workload,
        "env": env,
        "git_sha": git_sha,
        "captured_at": captured_at,
        "params": params,
        "kpis": [
            asdict(
                BaselineKPI(
                    name=k.name,
                    stat=k.stat,
                    value=k.value,
                    unit=k.unit,
                    direction=k.direction,
                    tolerance_pct=k.tolerance_pct,
                    tags=k.tags,
                )
            )
            for k in aggregated
        ],
    }
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)


def compare(
    aggregated: list[AggregatedKPI], baseline: Baseline
) -> tuple[bool, list[dict[str, Any]]]:
    """Compare aggregated KPIs against a baseline.

    Returns (all_pass, per_kpi_results). per_kpi_results includes both KPIs
    that exist in both, KPIs missing from baseline (treated as new, verdict
    "new"), and KPIs missing from current (verdict "missing").
    """
    by_name_current = {(k.name, _tag_key(k.tags)): k for k in aggregated}
    by_name_base = {(k.name, _tag_key(k.tags)): k for k in baseline.kpis}

    results: list[dict[str, Any]] = []
    all_pass = True

    for key, cur in by_name_current.items():
        base = by_name_base.get(key)
        if base is None:
            results.append(
                {
                    "name": cur.name,
                    "tags": cur.tags,
                    "current": cur.value,
                    "baseline": None,
                    "delta_pct": 0.0,
                    "verdict": "new",
                }
            )
            continue
        if base.value == 0:
            delta_pct = float("inf") if cur.value != 0 else 0.0
        else:
            delta_pct = (cur.value - base.value) / base.value * 100.0
        verdict = _verdict(delta_pct, base.direction, base.tolerance_pct)
        if verdict == "fail":
            all_pass = False
        results.append(
            {
                "name": cur.name,
                "tags": cur.tags,
                "current": cur.value,
                "baseline": base.value,
                "delta_pct": delta_pct,
                "verdict": verdict,
            }
        )

    for key, base in by_name_base.items():
        if key in by_name_current:
            continue
        all_pass = False
        results.append(
            {
                "name": base.name,
                "tags": base.tags,
                "current": None,
                "baseline": base.value,
                "delta_pct": 0.0,
                "verdict": "missing",
            }
        )

    return all_pass, results


def _verdict(delta_pct: float, direction: str, tolerance_pct: float) -> str:
    if direction == "lower_better":
        return "fail" if delta_pct > tolerance_pct else "pass"
    if direction == "higher_better":
        return "fail" if delta_pct < -tolerance_pct else "pass"
    raise ValueError(f"unknown direction {direction!r}")


def _tag_key(tags: dict[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(tags.items()))
