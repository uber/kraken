"""Devcluster environment adapter.

Drives the existing examples/devcluster/* start scripts via subprocess. All
ports come from the param scripts; the harness just talks to the published
host ports. Resource constraints are injected via KRAKEN_DOCKER_EXTRA_ARGS.
"""
from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass
from typing import Optional

import requests


# Devcluster port plan, lifted from examples/devcluster/{herd,agent_one,agent_two}_param.sh.
TESTFS_PORT = 14000
ORIGIN_SERVER_PORT = 15002
TRACKER_PORT = 15003
BUILD_INDEX_PORT = 15004
PROXY_PORT = 15000


def agent_registry_port(idx: int) -> int:
    """Registry port for agent N: 16000 (idx=1), 17000 (idx=2), 18000 (idx=3)..."""
    return 15000 + idx * 1000


def agent_admin_port(idx: int) -> int:
    return agent_registry_port(idx) + 2


def agent_container_name(idx: int) -> str:
    if idx == 1:
        return "kraken-agent-one"
    if idx == 2:
        return "kraken-agent-two"
    return f"kraken-agent-{idx}"


@dataclass
class DevclusterEnv:
    repo_root: str
    cpus: Optional[str] = None
    memory: Optional[str] = None
    agent_count: int = 0

    @property
    def docker_extra_args(self) -> str:
        parts = []
        if self.cpus:
            parts.append(f"--cpus={self.cpus}")
        if self.memory:
            parts.append(f"--memory={self.memory}")
            parts.append(f"--memory-swap={self.memory}")
        return " ".join(parts)

    def bring_up(self, agents: int = 2, ready_timeout: float = 120) -> None:
        if agents < 1:
            raise ValueError("need at least 1 agent")
        self.tear_down()
        self._run_script("herd_start_container.sh")
        if agents >= 1:
            self._run_script("agent_one_start_container.sh")
        if agents >= 2:
            self._run_script("agent_two_start_container.sh")
        for i in range(3, agents + 1):
            self._run_script("agent_n_start_container.sh", str(i))
        self.agent_count = agents
        self._wait_for_ready(timeout=ready_timeout)

    def tear_down(self) -> None:
        try:
            out = subprocess.check_output(
                ["docker", "ps", "-a", "--format", "{{.Names}}"]
            ).decode()
        except subprocess.CalledProcessError:
            return
        for name in out.splitlines():
            if name.startswith("kraken-"):
                subprocess.call(
                    ["docker", "rm", "-f", name], stderr=subprocess.DEVNULL
                )
        self.agent_count = 0

    def restart_agents(self) -> None:
        """Restart all agents to flush their per-agent caches and torrent state.

        This is the practical "cold cache" for pull workloads: the origin
        retains its CAStore (the realistic case for repeat pulls), while
        each agent re-fetches from origin/peers.
        """
        for i in range(1, self.agent_count + 1):
            subprocess.check_call(
                ["docker", "restart", agent_container_name(i)],
                stdout=subprocess.DEVNULL,
            )
        # Same race as components.py.Component.restart: brief sleep before health
        # checks to let the container actually come back up.
        time.sleep(1)
        self._wait_for_agents_ready()

    def _run_script(self, name: str, *args: str) -> None:
        env = os.environ.copy()
        env["KRAKEN_DOCKER_EXTRA_ARGS"] = self.docker_extra_args
        cmd = [os.path.join("examples/devcluster", name), *args]
        subprocess.check_call(cmd, cwd=self.repo_root, env=env)

    def testfs_url(self) -> str:
        return f"http://localhost:{TESTFS_PORT}"

    def agent_registry_url(self, idx: int) -> str:
        return f"http://localhost:{agent_registry_port(idx)}"

    def agent_admin_url(self, idx: int) -> str:
        return f"http://localhost:{agent_admin_port(idx)}"

    def upload_blob(self, name: str, blob: bytes) -> None:
        url = f"{self.testfs_url()}/files/blobs/{name}"
        r = requests.post(url, data=blob, timeout=300)
        r.raise_for_status()

    def download_blob(self, agent_idx: int, name: str, timeout: float = 600) -> int:
        """Stream a blob from an agent and return total bytes received."""
        url = f"{self.agent_admin_url(agent_idx)}/namespace/testfs/blobs/{name}"
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            total = 0
            for chunk in r.iter_content(chunk_size=256 * 1024):
                total += len(chunk)
        return total

    def pprof_targets(self) -> dict[str, tuple[str, int]]:
        """Component name -> (host, port) for /debug/pprof/* endpoints.

        Build-index does not register pprof handlers (no net/http/pprof import
        in build-index/), so it's intentionally omitted.
        """
        targets: dict[str, tuple[str, int]] = {
            "tracker": ("localhost", TRACKER_PORT),
            "origin": ("localhost", ORIGIN_SERVER_PORT),
            "proxy": ("localhost", PROXY_PORT),
        }
        for i in range(1, self.agent_count + 1):
            targets[f"agent-{i}"] = ("localhost", agent_admin_port(i))
        return targets

    def container_name(self, component: str) -> str:
        if component.startswith("agent-"):
            return agent_container_name(int(component.split("-", 1)[1]))
        if component in ("tracker", "origin", "proxy", "build-index", "testfs"):
            # All four herd-side components live in the single kraken-herd container.
            return "kraken-herd"
        raise KeyError(f"unknown component {component!r}")

    def docker_logs(self, container: str, tail: int = 200) -> str:
        try:
            return subprocess.check_output(
                ["docker", "logs", "--tail", str(tail), container],
                stderr=subprocess.STDOUT,
            ).decode(errors="replace")
        except subprocess.CalledProcessError as e:
            return f"<failed to read logs for {container}: {e}>"

    def _wait_for_ready(self, timeout: float) -> None:
        deadline = time.time() + timeout
        endpoints = [("testfs", f"{self.testfs_url()}/health")]
        for i in range(1, self.agent_count + 1):
            endpoints.append((f"agent-{i}", f"{self.agent_admin_url(i)}/health"))
        last_err: dict[str, str] = {}
        while time.time() < deadline:
            ok = True
            for name, url in endpoints:
                try:
                    r = requests.get(url, timeout=2)
                    if r.status_code != 200:
                        ok = False
                        last_err[name] = f"HTTP {r.status_code}"
                        break
                except Exception as e:
                    ok = False
                    last_err[name] = str(e)
                    break
            if ok:
                return
            time.sleep(2)
        raise TimeoutError(
            f"cluster not ready after {timeout}s; last errors: {last_err}"
        )

    def _wait_for_agents_ready(self, timeout: float = 60) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            ok = True
            for i in range(1, self.agent_count + 1):
                try:
                    r = requests.get(f"{self.agent_admin_url(i)}/health", timeout=2)
                    if r.status_code != 200:
                        ok = False
                        break
                except Exception:
                    ok = False
                    break
            if ok:
                return
            time.sleep(1)
        raise TimeoutError(f"agents not ready after {timeout}s")
