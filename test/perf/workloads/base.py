"""Workload base class and KPI specification."""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any


@dataclass
class KPISpec:
    """Specification for a single key performance indicator.

    `source` selects where the value is read from at the end of a run:
      - "harness": value is in the dict returned by Workload.run()
      - "metrics": value is scraped from the statsd_exporter (not yet wired
        in the devcluster slice; reserved for the follow-up)
      - "docker_stats": value is computed from `docker stats` snapshots

    `query` is source-specific:
      - harness: the dict key (e.g. "download_seconds")
      - metrics: the Prometheus metric name plus optional label filters
      - docker_stats: e.g. "kraken-agent-1.cpu_pct.max"

    `stat` is the aggregation applied to multi-sample sources (p50, p95, p99,
    mean, max, sum). It is ignored when the source naturally returns a scalar.
    """

    name: str
    source: str
    query: str
    stat: str = "p50"
    unit: str = ""
    direction: str = "lower_better"  # or "higher_better"
    tolerance_pct: float = 15.0
    tags: dict[str, str] = field(default_factory=dict)


class Workload(abc.ABC):
    """Base class for a benchmark workload.

    A workload encapsulates: how to prepare cluster state, how to drive load
    (the timed phase), what to measure, and how to clean up. Implementations
    must define `name`, `default_params`, and `run`. `setup`, `warmup`, and
    `teardown` have safe default implementations.
    """

    name: str = ""
    default_params: dict[str, Any] = {}

    def merge_params(self, overrides: dict[str, Any]) -> dict[str, Any]:
        merged = dict(self.default_params)
        merged.update(overrides)
        return merged

    def setup(self, env, params: dict[str, Any]) -> None:
        """Prepare cluster state before the timed phase. Default: no-op."""

    def warmup(self, env, params: dict[str, Any]) -> None:
        """Optional warmup before timing. Default: no-op."""

    @abc.abstractmethod
    def run(self, env, params: dict[str, Any]) -> dict[str, Any]:
        """Execute the timed phase. Must return a dict of harness-source
        measurements (floats or lists of floats) keyed by the names referenced
        from `kpis()`."""

    @abc.abstractmethod
    def kpis(self, params: dict[str, Any]) -> list[KPISpec]:
        """Return the KPIs this workload publishes for the given params.

        Params are passed in because tags often depend on them (e.g. the
        size_bucket label on size-bucketed histograms).
        """

    def teardown(self, env, params: dict[str, Any]) -> None:
        """Clean up workload-specific state. Default: no-op (the env handles
        cluster tear-down)."""

    def profile_targets(self, env, params: dict[str, Any]) -> list[str]:
        """Return component names whose pprof should be captured during the
        profile run. Default: all components the env knows about."""
        return list(env.pprof_targets().keys())
