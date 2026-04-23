"""Click-based CLI orchestrator for the perf benchmark harness.

See test/perf/README.md for usage. This first slice supports only the
devcluster env; the K8s adapter lands in a follow-up.
"""
from __future__ import annotations

import os
import sys
from typing import Any

import click

from . import baseline as baseline_mod
from . import registry, results
from .collect import MetricsCollector, ProfileCollector
from .envs.devcluster import DevclusterEnv


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_OUT = os.path.join(REPO_ROOT, "bench-results")
DEFAULT_BASELINES = os.path.join(REPO_ROOT, "test", "perf", "baselines")


def _parse_kv(s: str | None) -> dict[str, Any]:
    if not s:
        return {}
    out: dict[str, Any] = {}
    for chunk in s.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "=" not in chunk:
            raise click.BadParameter(f"expected key=value, got {chunk!r}")
        k, v = chunk.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _make_env(env_name: str, cpus: str | None, memory: str | None):
    if env_name == "devcluster":
        return DevclusterEnv(repo_root=REPO_ROOT, cpus=cpus, memory=memory)
    raise click.BadParameter(f"env {env_name!r} not implemented in this slice")


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--env", "env_name", default="devcluster",
              type=click.Choice(["devcluster"]),
              help="Target environment.")
@click.option("--workload", "workload_name", default=None,
              help="Workload name. Omit when using --bring-up or --tear-down only.")
@click.option("--runs", type=int, default=3, show_default=True,
              help="Number of timed runs to aggregate.")
@click.option("--params", default="", help="Workload params as key=value,key=value.")
@click.option("--tunables", default="",
              help="Config tunable overrides as key=value,key=value (reserved).")
@click.option("--baseline", "baseline_mode",
              type=click.Choice(["compare", "update", "skip"]),
              default="skip", show_default=True,
              help="Baseline behavior. compare gates exit on regression.")
@click.option("--profile/--no-profile", default=False,
              help="Run an additional profile-only pass and capture pprof.")
@click.option("--bring-up", "bring_up", is_flag=True,
              help="Bring up the cluster before running.")
@click.option("--tear-down", "tear_down", is_flag=True,
              help="Tear down the cluster (mutually exclusive with workload).")
@click.option("--agents", type=int, default=2, show_default=True,
              help="Agent count when bringing up.")
@click.option("--cpus", default=None,
              help="Per-container CPU limit, e.g. 2 (passed to docker --cpus).")
@click.option("--memory", default=None,
              help="Per-container memory limit, e.g. 4g.")
@click.option("--out", "out_dir", default=DEFAULT_OUT, show_default=True,
              help="Root directory for results.")
@click.option("--baselines-dir", default=DEFAULT_BASELINES, show_default=True,
              help="Root directory for baselines.")
@click.option("--cpu-seconds", type=int, default=30, show_default=True,
              help="Duration of CPU profile capture in the profile run.")
def main(
    env_name: str,
    workload_name: str | None,
    runs: int,
    params: str,
    tunables: str,
    baseline_mode: str,
    profile: bool,
    bring_up: bool,
    tear_down: bool,
    agents: int,
    cpus: str | None,
    memory: str | None,
    out_dir: str,
    baselines_dir: str,
    cpu_seconds: int,
) -> None:
    env = _make_env(env_name, cpus, memory)

    if tear_down and workload_name:
        raise click.BadParameter("--tear-down cannot be combined with --workload")

    if tear_down:
        env.tear_down()
        click.echo("Cluster torn down.")
        return

    if bring_up:
        click.echo(f"Bringing up devcluster with {agents} agents...")
        env.bring_up(agents=agents)
        click.echo("Cluster ready.")
        if not workload_name:
            return
    elif workload_name:
        # Surface a clearer error than a request timeout when the cluster
        # isn't running.
        env.agent_count = agents

    if not workload_name:
        raise click.UsageError(
            "no action requested; pass --workload, --bring-up, or --tear-down"
        )

    workload_cls = registry.get(workload_name)
    workload = workload_cls()
    merged_params = workload.merge_params(_parse_kv(params))
    parsed_tunables = _parse_kv(tunables)
    if parsed_tunables:
        click.echo(
            f"WARN: --tunables ignored in this slice (received {parsed_tunables})",
            err=True,
        )

    writer = results.ResultsWriter(
        out_root=out_dir,
        env_name=env_name,
        workload_name=workload_name,
        repo_root=REPO_ROOT,
    )
    run_dir = writer.new_run_dir()
    click.echo(f"Run dir: {run_dir}")
    writer.write_params(run_dir, merged_params, parsed_tunables)
    writer.write_env(run_dir, results.env_info(REPO_ROOT))

    metrics_dir = os.path.join(run_dir, "raw")
    metrics = MetricsCollector(out_dir=metrics_dir)

    click.echo("Workload setup...")
    workload.setup(env, merged_params)
    click.echo("Workload warmup...")
    workload.warmup(env, merged_params)

    metrics.start()
    raw_runs: list[dict[str, Any]] = []
    try:
        for i in range(runs):
            click.echo(f"Run {i + 1}/{runs}...")
            sample = workload.run(env, merged_params)
            click.echo(f"  -> {sample}")
            raw_runs.append(sample)
    finally:
        metrics.stop()

    aggregated = results.aggregate_kpis(raw_runs, workload.kpis(merged_params))

    if profile:
        profile_dir = os.path.join(run_dir, "profiles")
        os.makedirs(profile_dir, exist_ok=True)
        click.echo(f"Profile run: capturing pprof for {cpu_seconds}s...")
        profiler = ProfileCollector(out_dir=profile_dir, cpu_seconds=cpu_seconds)

        # Drive the workload again concurrently with the CPU profile so the CPU
        # sample window covers actual load. Run in a thread; profiler.capture
        # blocks for cpu_seconds.
        import threading

        def _drive_load() -> None:
            try:
                workload.run(env, merged_params)
            except Exception:
                pass

        loader = threading.Thread(target=_drive_load, daemon=True)
        loader.start()
        captured = profiler.capture(
            env.pprof_targets(),
            components=workload.profile_targets(env, merged_params),
        )
        loader.join(timeout=cpu_seconds + 60)
        click.echo(
            "Profiles captured: "
            + ", ".join(f"{c}={len(p)}" for c, p in captured.items() if p)
        )

    workload.teardown(env, merged_params)

    comparison = None
    exit_code = 0
    bpath = baseline_mod.baseline_path(baselines_dir, env_name, workload_name)

    if baseline_mode == "update":
        info = results.env_info(REPO_ROOT)
        baseline_mod.save(
            bpath,
            workload=workload_name,
            env=env_name,
            git_sha=info["git_sha"],
            captured_at=info["captured_at"],
            params=merged_params,
            aggregated=aggregated,
        )
        click.echo(f"Baseline updated: {bpath}")
    elif baseline_mode == "compare":
        existing = baseline_mod.load(bpath)
        if existing is None:
            click.echo(
                f"WARN: no baseline at {bpath}; skipping comparison.", err=True
            )
        else:
            all_pass, comparison = baseline_mod.compare(aggregated, existing)
            for r in comparison:
                click.echo(
                    "  {name} ({tags}): {cur} vs {base} ({delta:+.2f}%) -> {v}".format(
                        name=r["name"],
                        tags=r["tags"],
                        cur=r["current"],
                        base=r["baseline"],
                        delta=r["delta_pct"],
                        v=r["verdict"],
                    )
                )
            if not all_pass:
                exit_code = 1

    writer.write_metrics(run_dir, aggregated, raw_runs)
    writer.write_report(run_dir, aggregated, comparison)
    click.echo(f"Wrote {run_dir}")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
