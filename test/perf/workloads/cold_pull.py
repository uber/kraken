"""W1: cold-pull-large-blob workload.

Uploads a synthetic blob of configurable size to testfs, then times agent
pulls from a cold cache. "Cold" here means the agent containers are
restarted between runs (flushes torrent state and per-agent cache); origin
keeps its CAStore across runs (the realistic same-image-pulled-twice case
inside a session, not first-pull-from-backend).
"""
from __future__ import annotations

import hashlib
import random
import time
from typing import Any

from ..registry import register
from ..workloads.base import KPISpec, Workload


SIZE_SUFFIXES = {
    "B": 1,
    "KB": 1000,
    "MB": 1000 * 1000,
    "GB": 1000 * 1000 * 1000,
    "KIB": 1024,
    "MIB": 1024 * 1024,
    "GIB": 1024 * 1024 * 1024,
}


def parse_size(s: str | int) -> int:
    if isinstance(s, int):
        return s
    txt = str(s).strip()
    for suffix, mul in sorted(SIZE_SUFFIXES.items(), key=lambda x: -len(x[0])):
        if txt.upper().endswith(suffix):
            return int(float(txt[: -len(suffix)]) * mul)
    return int(txt)


def size_bucket(size_bytes: int) -> str:
    """Map a blob size to a coarse label that matches the histogram buckets
    emitted by the agent's download_time/throughput histograms."""
    mib = size_bytes / (1024 * 1024)
    if mib < 5:
        return "0-5MiB"
    if mib < 100:
        return "5-100MiB"
    if mib < 1024:
        return "100MiB-1GiB"
    if mib < 5 * 1024:
        return "1-5GiB"
    if mib < 10 * 1024:
        return "5-10GiB"
    return "10GiB+"


@register
class ColdPullLargeBlob(Workload):
    name = "cold-pull-large-blob"
    default_params: dict[str, Any] = {
        "blob_size": "256MiB",
        "agent_idx": 1,
        # Deterministic seed for blob generation so re-runs hit the same digest.
        "seed": 42,
    }

    def __init__(self) -> None:
        self._blob: bytes | None = None
        self._name: str | None = None
        self._size_bytes: int = 0
        self._run_idx: int = 0

    def setup(self, env, params: dict[str, Any]) -> None:
        self._size_bytes = parse_size(params["blob_size"])
        seed = int(params.get("seed", 42))
        # random.Random only accepts None/int/float/str/bytes/bytearray as a
        # seed, so derive a deterministic string from the inputs.
        rng = random.Random(f"{seed}:{self.name}:{self._size_bytes}")
        # random.Random.randbytes is available in 3.9+; both CI runners are 3.10+.
        self._blob = rng.randbytes(self._size_bytes)
        self._name = hashlib.sha256(self._blob).hexdigest()
        env.upload_blob(self._name, self._blob)

    def warmup(self, env, params: dict[str, Any]) -> None:
        # No warmup pull: that would warm the cache we explicitly want cold.
        # Restart agents once before the first timed run so the first sample
        # is not contaminated by whatever state the cluster came up in.
        env.restart_agents()

    def run(self, env, params: dict[str, Any]) -> dict[str, Any]:
        if self._blob is None or self._name is None:
            raise RuntimeError("setup() must be called before run()")
        agent_idx = int(params.get("agent_idx", 1))
        # Cold cache for every run.
        env.restart_agents()
        start = time.monotonic()
        bytes_received = env.download_blob(agent_idx, self._name)
        duration = time.monotonic() - start
        if bytes_received != self._size_bytes:
            raise RuntimeError(
                f"download size mismatch: got {bytes_received}, want {self._size_bytes}"
            )
        throughput_mibps = (bytes_received / (1024 * 1024)) / max(duration, 1e-9)
        self._run_idx += 1
        return {
            "download_seconds": duration,
            "download_bytes": float(bytes_received),
            "download_throughput_mibps": throughput_mibps,
        }

    def kpis(self, params: dict[str, Any]) -> list[KPISpec]:
        size_bytes = parse_size(params["blob_size"])
        bucket = size_bucket(size_bytes)
        common_tags = {"size_bucket": bucket}
        return [
            KPISpec(
                name="harness.download_time",
                source="harness",
                query="download_seconds",
                stat="p50",
                unit="s",
                direction="lower_better",
                tolerance_pct=15.0,
                tags=common_tags,
            ),
            KPISpec(
                name="harness.download_time",
                source="harness",
                query="download_seconds",
                stat="p95",
                unit="s",
                direction="lower_better",
                tolerance_pct=20.0,
                tags=common_tags,
            ),
            KPISpec(
                name="harness.download_time",
                source="harness",
                query="download_seconds",
                stat="p99",
                unit="s",
                direction="lower_better",
                tolerance_pct=25.0,
                tags=common_tags,
            ),
            KPISpec(
                name="harness.download_throughput",
                source="harness",
                query="download_throughput_mibps",
                stat="p50",
                unit="MiB/s",
                direction="higher_better",
                tolerance_pct=15.0,
                tags=common_tags,
            ),
            KPISpec(
                name="harness.download_throughput",
                source="harness",
                query="download_throughput_mibps",
                stat="p95",
                unit="MiB/s",
                direction="higher_better",
                tolerance_pct=20.0,
                tags=common_tags,
            ),
        ]
