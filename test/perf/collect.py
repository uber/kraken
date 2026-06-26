"""Profile and metric collectors for the perf harness.

Today the harness only captures pprof profiles plus the workload's own
end-to-end timings. Per-component statsd metrics will be wired once a
statsd_exporter sidecar lands; the MetricsCollector below is the seam.
"""
from __future__ import annotations

import concurrent.futures
import os
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Iterable

import requests


PPROF_KINDS = ("cpu", "heap", "mutex", "goroutine")


@dataclass
class ProfileCollector:
    """Snapshots /debug/pprof/* endpoints into a directory tree.

    Components without pprof (notably build-index) yield 404; those are
    skipped silently rather than failing the run. CPU profiles are sampled
    over `cpu_seconds`; the call blocks for that duration.
    """

    out_dir: str
    cpu_seconds: int = 30
    request_timeout: int = 60

    def capture(
        self,
        targets: dict[str, tuple[str, int]],
        components: Iterable[str] | None = None,
        kinds: Iterable[str] = PPROF_KINDS,
    ) -> dict[str, list[str]]:
        """Capture profiles in parallel, return component -> list of saved files."""
        wanted = list(components) if components is not None else list(targets.keys())
        kinds = list(kinds)
        results: dict[str, list[str]] = {c: [] for c in wanted}

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futs: dict[concurrent.futures.Future[str | None], tuple[str, str]] = {}
            for component in wanted:
                if component not in targets:
                    continue
                host, port = targets[component]
                for kind in kinds:
                    fut = pool.submit(self._fetch_one, component, host, port, kind)
                    futs[fut] = (component, kind)
            for fut in concurrent.futures.as_completed(futs):
                component, _kind = futs[fut]
                path = fut.result()
                if path:
                    results[component].append(path)
        return results

    def _fetch_one(
        self, component: str, host: str, port: int, kind: str
    ) -> str | None:
        path = f"debug/pprof/{kind}"
        if kind == "cpu":
            # The pprof CPU endpoint is /debug/pprof/profile, not /debug/pprof/cpu.
            path = "debug/pprof/profile"
            url = f"http://{host}:{port}/{path}?seconds={self.cpu_seconds}"
            timeout = self.cpu_seconds + 30
        else:
            url = f"http://{host}:{port}/{path}"
            timeout = self.request_timeout
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 404:
                return None
            r.raise_for_status()
        except requests.RequestException:
            return None
        comp_dir = os.path.join(self.out_dir, component)
        os.makedirs(comp_dir, exist_ok=True)
        out_path = os.path.join(comp_dir, f"{kind}.pb.gz")
        with open(out_path, "wb") as f:
            f.write(r.content)
        return out_path


@dataclass
class MetricsCollector:
    """Polls a Prometheus-format /metrics endpoint at a fixed cadence.

    The first slice of the harness ships with no metrics endpoint (devcluster
    does not run a statsd_exporter sidecar). When `endpoint` is None, start()
    and stop() are no-ops, snapshot() returns an empty dict, and aggregate()
    returns an empty dict. The follow-up that wires statsd_exporter just needs
    to pass an endpoint URL to the constructor.
    """

    out_dir: str
    endpoint: str | None = None
    interval_seconds: float = 1.0
    _stop: threading.Event = field(default_factory=threading.Event, init=False)
    _thread: threading.Thread | None = field(default=None, init=False)
    _scrape_paths: list[str] = field(default_factory=list, init=False)

    def start(self) -> None:
        if self.endpoint is None:
            return
        os.makedirs(self.out_dir, exist_ok=True)
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self, flush_grace_seconds: float = 10.0) -> None:
        if self.endpoint is None or self._thread is None:
            return
        # Sleep for the statsd_exporter's flush interval so the final scrape
        # captures tail counters that were emitted just before stop().
        time.sleep(flush_grace_seconds)
        self._scrape_once()
        self._stop.set()
        self._thread.join()
        self._thread = None

    def snapshot(self) -> str | None:
        if self.endpoint is None:
            return None
        return self._scrape_once()

    def aggregate(self, kpis: list) -> dict:  # noqa: ARG002
        # Reserved: parse the most recent scrape, derive p50/p95/p99 from
        # _bucket{le=...} series, compute counter rates. Returns empty until
        # the statsd_exporter is wired.
        return {}

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._scrape_once()
            self._stop.wait(self.interval_seconds)

    def _scrape_once(self) -> str | None:
        if self.endpoint is None:
            return None
        try:
            r = requests.get(self.endpoint, timeout=5)
            r.raise_for_status()
        except requests.RequestException:
            return None
        ts = int(time.time())
        path = os.path.join(self.out_dir, f"metrics-{ts}.prom")
        with open(path, "wb") as f:
            f.write(r.content)
        self._scrape_paths.append(path)
        return path


def _quote(s: str) -> str:
    """URL-quote a path segment for callers that need it."""
    return urllib.parse.quote(s, safe="")
