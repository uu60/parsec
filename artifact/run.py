#!/usr/bin/env python3
"""Unified, provenance-preserving runner for the ParsecDB artifact."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import re
import shlex
import socket
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence


ARTIFACT_DIR = Path(__file__).resolve().parent
REPO_ROOT = ARTIFACT_DIR.parent
EXPERIMENT_SPEC = ARTIFACT_DIR / "experiments.yaml"
EXPECTED_SMOKE_IDS = list(range(1, 9))
DEFAULT_SEED = 20270276
PAPER_TIMEOUT_SECONDS = 86400
FIXED_REPETITIONS = 1
DEFAULT_MPI_ARGS = [
    "--bind-to", "none",
    "--map-by", "seq",
    "--host", "parsec0,parsec1,parsec0",
]
METRIC_PREFIX = "ARTIFACT_METRIC "
MICRO_METRIC_PREFIX = "ARTIFACT_MICRO_METRIC "
ACTIVE_OUTPUT_DIR: Path | None = None
ACTIVE_MANIFEST: dict[str, Any] | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def command_text(command: Sequence[str]) -> str:
    return shlex.join(str(part) for part in command)


def capture(command: Sequence[str]) -> str:
    try:
        completed = subprocess.run(
            [str(part) for part in command], cwd=REPO_ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, timeout=10, check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return f"unavailable: {exc}"
    output = completed.stdout.strip()
    if completed.returncode != 0:
        return f"exit code {completed.returncode}" + (f": {output}" if output else "")
    return output


def first_line(value: str) -> str:
    return value.splitlines()[0] if value else "unknown"


def cpu_model() -> str:
    if sys.platform == "darwin":
        model = capture(["sysctl", "-n", "machdep.cpu.brand_string"])
        if not model.startswith(("unavailable:", "exit code")):
            return model
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.is_file():
        for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.lower().startswith("model name") and ":" in line:
                return line.split(":", 1)[1].strip()
    return platform.processor() or platform.machine() or "unknown"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def plotter_metadata(experiment: str) -> dict[str, Any]:
    """Record both the normalized-data adapter and selected parsec_charts script."""
    try:
        from parsec_charts import script_path
    except ModuleNotFoundError:
        from artifact.parsec_charts import script_path
    adapter = ARTIFACT_DIR / "plotting.py"
    script = script_path(experiment)
    return {
        "path": str(adapter),
        "sha256": sha256(adapter),
        "parsec_charts_script": str(script),
        "parsec_charts_sha256": sha256(script),
    }


def git_metadata() -> dict[str, Any]:
    status = capture(["git", "status", "--porcelain=v1", "--untracked-files=all"])
    valid = not status.startswith(("unavailable:", "exit code"))
    lines = status.splitlines() if valid and status else []
    return {
        "commit": capture(["git", "rev-parse", "HEAD"]),
        "branch": capture(["git", "branch", "--show-current"]),
        "dirty": bool(lines) if valid else None,
        "status": lines,
    }


def environment_metadata() -> dict[str, Any]:
    cache_values: dict[str, str] = {}
    cache_path = REPO_ROOT / "build" / "CMakeCache.txt"
    if cache_path.is_file():
        wanted = {"CMAKE_BUILD_TYPE", "CMAKE_CXX_COMPILER", "CMAKE_CXX_FLAGS_RELEASE", "PARSEC_SIMD_TARGET"}
        for line in cache_path.read_text(encoding="utf-8", errors="replace").splitlines():
            if ":" in line and "=" in line:
                key = line.split(":", 1)[0]
                if key in wanted:
                    cache_values[key] = line.split("=", 1)[1]
    return {
        "hostname": socket.gethostname(),
        "operating_system": platform.platform(),
        "architecture": platform.machine(),
        "cpu_model": cpu_model(),
        "logical_cpu_count": os.cpu_count(),
        "python": sys.version.splitlines()[0],
        "compiler": first_line(capture(["c++", "--version"])),
        "cmake": first_line(capture(["cmake", "--version"])),
        "ninja": first_line(capture(["ninja", "--version"])),
        "mpi": first_line(capture(["mpirun", "--version"])),
        "cmake_cache": cache_values,
    }


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_spec() -> dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError(
            "performance workflows require PyYAML; install artifact/requirements.txt"
        ) from exc
    with EXPERIMENT_SPEC.open("r", encoding="utf-8") as stream:
        value = yaml.safe_load(stream)
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise RuntimeError("unsupported or invalid artifact/experiments.yaml")
    return value


def prepare_result_directory(experiment: str, explicit: str | None) -> Path:
    if explicit:
        requested = Path(explicit)
        path = requested if requested.is_absolute() else REPO_ROOT / requested
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = ARTIFACT_DIR / "results" / f"{stamp}-{experiment}"
    if path.exists() and any(path.iterdir()):
        raise ValueError(f"result directory is not empty: {path}")
    for child in ("raw", "summary", "figures"):
        (path / child).mkdir(parents=True, exist_ok=True)
    return path


def manifest_base(experiment: str, output_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "artifact": "ParsecDB",
        "experiment": experiment,
        "status": "running",
        "started_at": utc_now(),
        "output_directory": str(output_dir),
        "experiment_spec": {
            "path": str(EXPERIMENT_SPEC.relative_to(REPO_ROOT)),
            "sha256": sha256(EXPERIMENT_SPEC),
        },
        "source": git_metadata(),
        "environment": environment_metadata(),
        "arguments": {key: value for key, value in vars(args).items() if key != "handler"},
        "input_scale_locked": True,
        "paper_scale_locked": True,
        "steps": [],
    }


def run_logged(command: Sequence[str], log_path: Path, timeout: float | None = None) -> dict[str, Any]:
    printable = command_text(command)
    print(f"$ {printable}", flush=True)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            [str(part) for part in command], cwd=REPO_ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, timeout=timeout, check=False,
        )
        output = completed.stdout
        code = completed.returncode
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        output = (exc.stdout or "") + (exc.stderr or "")
        code = 124
        timed_out = True
    except OSError as exc:
        output = f"failed to start command: {exc}\n"
        code = 127
        timed_out = False
    log_path.write_text(f"$ {printable}\n{output}", encoding="utf-8")
    if output:
        print(output, end="" if output.endswith("\n") else "\n", flush=True)
    return {
        "command": printable,
        "return_code": code,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "timed_out": timed_out,
        "log": str(log_path),
        "output": output,
    }


def build_targets(targets: Iterable[str], output_dir: Path, args: argparse.Namespace) -> dict[str, Any] | None:
    if args.skip_build:
        missing = [target for target in targets if not target_path(target).is_file()]
        if missing:
            raise RuntimeError(f"--skip-build used but binaries are missing: {', '.join(missing)}")
        return None
    unique = list(dict.fromkeys(targets))
    if (REPO_ROOT / "build" / "CMakeCache.txt").is_file():
        configure = run_logged(
            ["cmake", "-S", ".", "-B", "build", f"-DPARSEC_SIMD_TARGET={args.simd_target}"],
            output_dir / "raw" / "configure.log",
        )
        if configure["return_code"] != 0:
            raise RuntimeError("CMake configuration failed; see raw/configure.log")
        command = ["cmake", "--build", "build", "--target", *unique, "-j", str(args.jobs)]
    else:
        command = ["sh", "build.sh", f"--{args.build_mode}", f"--simd={args.simd_target}"]
    step = run_logged(command, output_dir / "raw" / "build.log")
    if step["return_code"] != 0:
        raise RuntimeError("build failed; see raw/build.log")
    result = {key: value for key, value in step.items() if key != "output"}
    if 'configure' in locals():
        result["configure"] = {key: value for key, value in configure.items() if key != "output"}
    return result


def target_path(target: str) -> Path:
    if target.startswith("exp_"):
        return REPO_ROOT / "build" / "db" / "exp" / target
    if target.startswith("benchmark_"):
        return REPO_ROOT / "build" / "primitives" / "benchmark" / target
    return REPO_ROOT / "build" / "db" / "benchmark" / target


def parse_metrics(text: str) -> list[dict[str, Any]]:
    metrics = []
    for line in text.splitlines():
        position = line.find(METRIC_PREFIX)
        if position < 0:
            continue
        payload = line[position + len(METRIC_PREFIX):].strip()
        try:
            metric = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"malformed ARTIFACT_METRIC line: {exc}") from exc
        metrics.append(metric)
    return metrics


def parse_micro_metrics(text: str) -> list[dict[str, Any]]:
    metrics = []
    for line in text.splitlines():
        position = line.find(MICRO_METRIC_PREFIX)
        if position < 0:
            continue
        payload = line[position + len(MICRO_METRIC_PREFIX):].strip()
        try:
            metric = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"malformed ARTIFACT_MICRO_METRIC line: {exc}") from exc
        metrics.append(metric)
    return metrics


def is_known_background_teardown_failure(
    executable: Path, return_code: int, output: str, metrics: list[dict[str, Any]],
) -> bool:
    """Accept only the known post-metric Background/MPI_Finalize heap failure.

    The paper's original Python driver stopped each process group as soon as it
    parsed RESULT.  The standalone background executable can otherwise reach a
    known teardown race after emitting its complete metric.  Keep the original
    nonzero exit in result metadata, but never relax failures before the metric
    or failures without the exact MPI_Finalize/heap-corruption signature.
    """
    if executable.name != "benchmark_background_single" or return_code != 134 or len(metrics) != 1:
        return False
    metric_position = output.rfind(MICRO_METRIC_PREFIX)
    if metric_position < 0:
        return False
    teardown = output[metric_position:]
    return all(marker in teardown for marker in (
        "malloc_consolidate(): unaligned fastbin chunk detected",
        "ompi_mpi_finalize",
        "exited on signal 6",
    ))


def run_tcp(
    executable: Path, params: list[str], base_port: int, timeout: float, log_prefix: Path
) -> tuple[int, str, str, float]:
    processes: list[tuple[int, subprocess.Popen[Any], list[str], Path, Any]] = []
    started = time.monotonic()
    try:
        for rank in range(3):
            command = [
                str(executable), *params, "--comm_type=tcp", f"--tcp_rank={rank}",
                "--tcp_size=3", "--tcp_host=127.0.0.1", f"--tcp_base_port={base_port}",
            ]
            log_path = log_prefix.parent / f"{log_prefix.name}-rank{rank}.log"
            log_stream = log_path.open("w", encoding="utf-8")
            log_stream.write(f"$ {command_text(command)}\n")
            log_stream.flush()
            process = subprocess.Popen(
                command, cwd=REPO_ROOT, stdout=log_stream,
                stderr=subprocess.STDOUT, text=True,
            )
            processes.append((rank, process, command, log_path, log_stream))
            time.sleep(0.1)

        deadline = started + timeout
        for rank, process, _, _, _ in processes:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                for _, candidate, _, _, _ in processes:
                    if candidate.poll() is None:
                        candidate.kill()
                raise RuntimeError(f"TCP run timed out after {timeout} seconds")
    finally:
        for _, process, _, _, log_stream in processes:
            if process.poll() is None:
                process.kill()
                process.wait()
            log_stream.close()

    combined_parts = []
    commands = []
    return_code = 0
    for rank, process, command, log_path, _ in processes:
        full_log = log_path.read_text(encoding="utf-8", errors="replace")
        output = full_log.split("\n", 1)[1] if "\n" in full_log else ""
        code = process.returncode
        combined_parts.append(f"===== rank {rank} exit={code} =====\n{output}")
        commands.append(command_text(command))
        if code != 0:
            return_code = code
    combined = "\n".join(combined_parts)
    return return_code, combined, " | ".join(commands), time.monotonic() - started


def run_mpc(
    *, executable: Path, params: list[str], args: argparse.Namespace,
    output_dir: Path, run_name: str, port_offset: int,
) -> dict[str, Any]:
    print(f"[run] {run_name}", flush=True)
    started = time.monotonic()
    if args.comm == "tcp":
        code, output, command, elapsed = run_tcp(
            executable, params, args.tcp_base_port + port_offset * 10,
            args.timeout, output_dir / "raw" / run_name,
        )
        log_reference = f"raw/{run_name}-rank{{0,1,2}}.log"
    else:
        command_parts = [args.mpirun, *args.mpi_arg, "-np", "3", str(executable), *params]
        result = run_logged(command_parts, output_dir / "raw" / f"{run_name}.log", args.timeout)
        code, output, command, elapsed = (
            result["return_code"], result["output"], result["command"], result["elapsed_seconds"]
        )
        log_reference = f"raw/{run_name}.log"
    if code != 0:
        raise RuntimeError(f"MPC run failed ({run_name}, exit {code}); see {log_reference}")

    metrics = parse_metrics(output)
    by_rank = {int(metric["rank"]): metric for metric in metrics}
    if set(by_rank) != {0, 1, 2}:
        raise RuntimeError(f"{run_name}: expected metrics from ranks 0,1,2; got {sorted(by_rank)}")
    server_metrics = [by_rank[0], by_rank[1]]
    elapsed_seconds = max(float(item["elapsed_seconds"]) for item in server_metrics)
    bmt_values = [float(item.get("bmt_generator_accumulated_seconds", -1)) for item in server_metrics]
    bmt_seconds = max(bmt_values) if min(bmt_values) >= 0 else None
    online_seconds = elapsed_seconds - bmt_seconds if bmt_seconds is not None and bmt_seconds <= elapsed_seconds else None
    return {
        "command": command,
        "return_code": code,
        "launcher_elapsed_seconds": round(float(elapsed), 6),
        "elapsed_seconds": elapsed_seconds,
        "bmt_generator_accumulated_seconds": bmt_seconds,
        "online_without_bmt_seconds": online_seconds,
        "output_rows": max(int(item["output_rows"]) for item in server_metrics),
        "rank_metrics": [by_rank[index] for index in sorted(by_rank)],
        "log": log_reference,
    }


def run_microbenchmark(
    *, experiment: str, executable: Path, params: list[str], args: argparse.Namespace,
    output_dir: Path, run_name: str, port_offset: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run a three-rank primitive benchmark and collect client-emitted records."""
    print(f"[run] {run_name}", flush=True)
    if args.comm == "tcp":
        code, output, command, elapsed = run_tcp(
            executable, params, args.tcp_base_port + port_offset * 10,
            args.timeout, output_dir / "raw" / run_name,
        )
        log_reference = f"raw/{run_name}-rank{{0,1,2}}.log"
    else:
        command_parts = [args.mpirun, *args.mpi_arg, "-np", "3", str(executable), *params]
        result = run_logged(command_parts, output_dir / "raw" / f"{run_name}.log", args.timeout)
        code, output, command, elapsed = (
            result["return_code"], result["output"], result["command"], result["elapsed_seconds"]
        )
        log_reference = f"raw/{run_name}.log"
    metrics = parse_micro_metrics(output)
    ignored_teardown = is_known_background_teardown_failure(executable, code, output, metrics)
    if code != 0 and not ignored_teardown:
        raise RuntimeError(f"microbenchmark failed ({run_name}, exit {code}); see {log_reference}")
    if not metrics:
        raise RuntimeError(f"{run_name}: benchmark emitted no {MICRO_METRIC_PREFIX.strip()} records")
    wrong = sorted({str(metric.get("experiment")) for metric in metrics if metric.get("experiment") != experiment})
    if wrong:
        raise RuntimeError(f"{run_name}: unexpected experiment identifiers: {wrong}")
    if ignored_teardown:
        print(
            f"warning: {run_name} emitted a valid metric before the known Background "
            f"MPI_Finalize teardown failure (exit {code}); retaining the metric",
            flush=True,
        )
    launch = {
        "command": command,
        "return_code": code,
        "teardown_failure_ignored": ignored_teardown,
        "launcher_elapsed_seconds": round(float(elapsed), 6),
        "log": log_reference,
    }
    return metrics, launch


def configuration_params(name: str, seed: int, collect_bmt: bool) -> list[str]:
    values = {
        "parsec": {"baseline_mode": "false", "no_compaction": "false", "batch_size": "256"},
        "parsec_base": {"baseline_mode": "true", "no_compaction": "true", "batch_size": "0"},
        "parsec_noncompact": {"baseline_mode": "false", "no_compaction": "true", "batch_size": "256"},
    }
    if name not in values:
        raise RuntimeError(
            f"configuration {name!r} is external; archive and run its original artifact separately"
        )
    params = [f"--{key}={value}" for key, value in values[name].items()]
    params.extend([f"--workload_seed={seed}", f"--enable_class_wise_timing={'true' if collect_bmt else 'false'}"])
    return params


def arg_params(values: dict[str, Any]) -> list[str]:
    return [f"--{key}={str(value).lower() if isinstance(value, bool) else value}" for key, value in values.items()]


def aggregate(records: list[dict[str, Any]], dimensions: list[str]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        groups[tuple(record.get(name) for name in dimensions)].append(record)
    summaries = []
    for key, items in groups.items():
        elapsed = [float(item["elapsed_seconds"]) for item in items]
        row = {name: value for name, value in zip(dimensions, key)}
        row.update({
            "repetitions": len(items),
            "mean_elapsed_seconds": statistics.fmean(elapsed),
            "stdev_elapsed_seconds": statistics.stdev(elapsed) if len(elapsed) > 1 else 0.0,
            "min_elapsed_seconds": min(elapsed),
            "max_elapsed_seconds": max(elapsed),
            "output_rows": max(int(item["output_rows"]) for item in items),
        })
        bmt = [item["bmt_generator_accumulated_seconds"] for item in items]
        valid_bmt = [float(value) for value in bmt if value is not None]
        row["mean_bmt_generator_accumulated_seconds"] = (
            statistics.fmean(valid_bmt) if len(valid_bmt) == len(items) else None
        )
        online = [item["online_without_bmt_seconds"] for item in items]
        valid_online = [float(value) for value in online if value is not None]
        row["mean_online_without_bmt_seconds"] = (
            statistics.fmean(valid_online) if len(valid_online) == len(items) else None
        )
        summaries.append(row)
    # dict preserves first-seen order, which is the experiment matrix order.
    # This matters for powers-of-two axes where lexical sorting is incorrect.
    return summaries


def aggregate_microbenchmarks(
    records: list[dict[str, Any]], dimensions: list[str], measurements: list[str], experiment: str,
) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        groups[tuple(record.get(name) for name in dimensions)].append(record)
    summaries = []
    for key, items in groups.items():
        row = {name: value for name, value in zip(dimensions, key)}
        row["repetitions"] = len(items)
        for measurement in measurements:
            values = [float(item[measurement]) for item in items]
            row[f"mean_{measurement}"] = statistics.fmean(values)
            row[f"stdev_{measurement}"] = statistics.stdev(values) if len(values) > 1 else 0.0
            row[f"min_{measurement}"] = min(values)
            row[f"max_{measurement}"] = max(values)
        if experiment == "figure2":
            denominator = row["mean_boolean_ms"]
            row["arithmetic_over_boolean"] = (
                row["mean_arithmetic_ms"] / denominator if denominator > 0 else None
            )
        elif experiment == "figure4":
            denominator = row["mean_jit_ms"]
            row["background_over_jit"] = (
                row["mean_background_ms"] / denominator if denominator > 0 else None
            )
        summaries.append(row)
    return summaries


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("\n", encoding="utf-8")
        return
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
                for key, value in row.items()
            })


def finalize_performance(
    *, experiment: str, records: list[dict[str, Any]], dimensions: list[str],
    output_dir: Path, manifest: dict[str, Any], x_field: str, series_field: str,
) -> int:
    summary_rows = aggregate(records, dimensions)
    summary = {
        "schema_version": 1, "experiment": experiment, "status": "passed",
        "measurement": "arithmetic mean; each run is max synchronized elapsed time of server ranks 0 and 1",
        "records": records, "aggregates": summary_rows,
    }
    write_json(output_dir / "summary" / f"{experiment}.json", summary)
    write_csv(output_dir / "summary" / f"{experiment}-raw.csv", records)
    write_csv(output_dir / "summary" / f"{experiment}.csv", summary_rows)
    aggregate_csv = output_dir / "summary" / f"{experiment}.csv"
    # Persist the usable measurements before plotting.  If the Python plotting
    # environment is incomplete, reviewers can repair it and run `plot` without
    # rerunning an expensive MPC matrix.
    manifest.update({
        "status": "finalizing",
        "data_status": "passed",
        "summary": f"summary/{experiment}.json",
        "raw_csv": f"summary/{experiment}-raw.csv",
        "aggregate_csv": f"summary/{experiment}.csv",
        "run_count": len(records),
    })
    write_json(output_dir / "manifest.json", manifest)
    try:
        from plotting import generate_plots
    except ModuleNotFoundError:
        from artifact.plotting import generate_plots

    figure_paths = generate_plots(
        experiment, [aggregate_csv], output_dir / "figures",
        profile=manifest.get("arguments", {}).get("profile"),
    )
    manifest.update({
        "status": "passed", "completed_at": utc_now(),
        "figures": [str(path.relative_to(output_dir)) for path in figure_paths],
        "figure": f"figures/{experiment}.svg",
        "plotter": {
            **plotter_metadata(experiment),
            "source_fields": {"x": x_field, "series": series_field},
        },
    })
    write_json(output_dir / "manifest.json", manifest)
    print(f"Artifact result: passed\nResults written to: {output_dir}", flush=True)
    return 0


def finalize_microbenchmark(
    *, experiment: str, records: list[dict[str, Any]], dimensions: list[str],
    measurements: list[str], output_dir: Path, manifest: dict[str, Any],
) -> int:
    summary_rows = aggregate_microbenchmarks(records, dimensions, measurements, experiment)
    summary = {
        "schema_version": 1,
        "experiment": experiment,
        "status": "passed",
        "measurement": (
            "Each C++ point is the arithmetic mean of server-rank 0 and 1 elapsed milliseconds; "
            "aggregate fields are arithmetic means across independent benchmark invocations."
        ),
        "records": records,
        "aggregates": summary_rows,
    }
    write_json(output_dir / "summary" / f"{experiment}.json", summary)
    write_csv(output_dir / "summary" / f"{experiment}-raw.csv", records)
    aggregate_csv = output_dir / "summary" / f"{experiment}.csv"
    write_csv(aggregate_csv, summary_rows)
    manifest.update({
        "status": "finalizing",
        "data_status": "passed",
        "summary": f"summary/{experiment}.json",
        "raw_csv": f"summary/{experiment}-raw.csv",
        "aggregate_csv": f"summary/{experiment}.csv",
        "run_count": len(records),
    })
    write_json(output_dir / "manifest.json", manifest)
    try:
        from plotting import generate_plots
    except ModuleNotFoundError:
        from artifact.plotting import generate_plots
    figure_paths = generate_plots(
        experiment, [aggregate_csv], output_dir / "figures",
        profile=manifest.get("arguments", {}).get("profile"),
    )
    manifest.update({
        "status": "passed",
        "completed_at": utc_now(),
        "figures": [str(path.relative_to(output_dir)) for path in figure_paths],
        "figure": f"figures/{experiment}.svg",
        "plotter": {
            **plotter_metadata(experiment),
            "source_fields": {"x": "data_scale", "series": "primitive"},
        },
    })
    write_json(output_dir / "manifest.json", manifest)
    print(f"Artifact result: passed\nResults written to: {output_dir}", flush=True)
    return 0


def performance_context(experiment: str, args: argparse.Namespace) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    global ACTIVE_OUTPUT_DIR, ACTIVE_MANIFEST
    output_dir = prepare_result_directory(experiment, args.output_dir)
    manifest = manifest_base(experiment, output_dir, args)
    ACTIVE_OUTPUT_DIR, ACTIVE_MANIFEST = output_dir, manifest
    write_json(output_dir / "manifest.json", manifest)
    return output_dir, manifest, load_spec()


def repetitions(args: argparse.Namespace, paper_default: int = 3) -> int:
    return FIXED_REPETITIONS


def micro_base_elements(profile: dict[str, Any], primitive: str) -> int:
    if primitive == "sort":
        return int(profile.get("sort_base_elements", profile["base_elements"]))
    return int(profile["base_elements"])


def paper_micro_points(profile: dict[str, Any]) -> list[tuple[str, int, int]]:
    """Return only points consumed by the paper plots.

    Every input size is measured at 64 bits. The middle input size additionally
    supplies the 16- and 32-bit width-sensitivity points; its 64-bit point is not
    duplicated.
    """
    points: list[tuple[str, int, int]] = []
    widths = [int(value) for value in profile["widths"]]
    if 64 not in widths:
        raise RuntimeError("paper microbenchmark matrix requires width=64")
    for primitive_value in profile["primitives"]:
        primitive = str(primitive_value)
        elements_grid = profile["sort_nums"] if primitive == "sort" else profile["nums"]
        middle = int(elements_grid[len(elements_grid) // 2])
        for elements_value in elements_grid:
            elements = int(elements_value)
            selected_widths = widths if elements == middle else [64]
            for width in selected_widths:
                points.append((primitive, elements, width))
    return points


def run_matrix(
    cases: list[dict[str, Any]], args: argparse.Namespace, output_dir: Path,
    manifest: dict[str, Any], targets: list[str], collect_bmt: bool = False,
) -> list[dict[str, Any]]:
    step = build_targets(targets, output_dir, args)
    if step:
        step["name"] = "build"
        manifest["steps"].append(step)
        manifest["environment_after_build"] = environment_metadata()
        write_json(output_dir / "manifest.json", manifest)
    records = []
    for index, case in enumerate(cases):
        params = configuration_params(case["configuration"], args.seed, collect_bmt)
        params.extend(arg_params(case["params"]))
        run_name = f"{index:04d}-{case['label']}-r{case['repetition']}"
        result = run_mpc(
            executable=target_path(case["target"]), params=params, args=args,
            output_dir=output_dir, run_name=run_name, port_offset=index,
        )
        record = {key: value for key, value in case.items() if key not in {"params", "target", "label"}}
        record.update({
            "workload_seed": args.seed, "parameters": case["params"],
            **{key: value for key, value in result.items() if key != "rank_metrics"},
            "rank_metrics": result["rank_metrics"],
        })
        records.append(record)
        write_json(output_dir / "summary" / "checkpoint.json", records)
    return records


def run_micro_figure(
    args: argparse.Namespace, *, experiment: str, spec_key: str, target: str,
    measurements: list[str], dimensions: list[str],
) -> int:
    output_dir, manifest, spec = performance_context(experiment, args)
    experiment_spec = spec["experiments"][spec_key]
    original_profile = experiment_spec["profiles"]["paper"]
    profile = dict(original_profile)
    step = build_targets([target], output_dir, args)
    if step:
        step["name"] = "build"
        manifest["steps"].append(step)
        manifest["environment_after_build"] = environment_metadata()
        write_json(output_dir / "manifest.json", manifest)

    points: list[tuple[str, int, int, int | None]] = []
    batch_grid: list[int | None] = [
        int(value) for value in profile.get("batch_sizes", [profile.get("batch_size")])
    ]
    for primitive, elements, width in paper_micro_points(profile):
        for batch_size in batch_grid:
            points.append((primitive, elements, width, batch_size))
    manifest["experiment_matrix"] = profile
    manifest["paper_experiment_matrix"] = original_profile
    manifest["input_scale_locked"] = True
    manifest["paper_scale_locked"] = True
    manifest["execution_strategy"] = (
        "Each microbenchmark point runs in a fresh three-rank MPI process group and is "
        "checkpointed before the next point starts."
    )
    manifest["progress"] = {"completed_points": 0, "total_points": len(points), "current_point": None}
    write_json(output_dir / "manifest.json", manifest)

    records: list[dict[str, Any]] = []
    primitive_labels = {"<": "gt", "!=": "neq", "==": "eq", "ar": "ar", "mux": "mux", "sort": "sort"}
    for point_index, (primitive, elements, width, batch_size) in enumerate(points, start=1):
        point = {
            "index": point_index, "primitive": primitive, "elements": elements,
            "width": width, "batch_size": batch_size,
        }
        manifest["progress"] = {
            "completed_points": len(records), "total_points": len(points), "current_point": point,
        }
        write_json(output_dir / "manifest.json", manifest)
        batch_text = f" batch_size={batch_size}" if batch_size is not None else ""
        print(
            f"[progress] {experiment} point {point_index}/{len(points)}: "
            f"primitive={primitive} elements={elements} width={width}{batch_text}",
            flush=True,
        )
        params: dict[str, Any] = {
            "nums": elements, "sort_nums": elements, "widths": width, "pmts": primitive,
            "artifact_mode": True, "workload_seed": args.seed,
        }
        if "batch_sizes" in profile:
            params["batch_sizes"] = batch_size
        elif batch_size is not None:
            params["batch_size"] = batch_size
        label = f"{primitive_labels.get(primitive, primitive)}-{elements}-w{width}"
        if batch_size is not None:
            label += f"-b{batch_size}"
        metrics, launch = run_microbenchmark(
            experiment=experiment,
            executable=target_path(target),
            params=arg_params(params),
            args=args,
            output_dir=output_dir,
            run_name=f"{point_index:03d}-{label}",
            port_offset=point_index - 1,
        )
        if len(metrics) != 1:
            raise RuntimeError(f"{label}: expected exactly one {experiment} metric, got {len(metrics)}")
        metric = metrics[0]
        observed = (
            str(metric.get("primitive")), int(metric.get("elements", -1)),
            int(metric.get("width", -1)),
        )
        if observed != (primitive, elements, width):
            raise RuntimeError(
                f"{label}: metric identity mismatch: expected={(primitive, elements, width)}, observed={observed}"
            )
        if batch_size is not None and int(metric.get("batch_size", -1)) != batch_size:
            raise RuntimeError(f"{label}: batch-size metric identity mismatch")
        record = dict(metric)
        record.update({
            "repetition": 1, "workload_seed": args.seed,
            "data_scale": float(elements) / float(micro_base_elements(profile, primitive)),
            **launch,
        })
        records.append(record)
        write_json(output_dir / "summary" / "checkpoint.json", records)
        manifest["progress"] = {
            "completed_points": len(records), "total_points": len(points), "current_point": None,
            "last_completed_point": point, "updated_at": utc_now(),
        }
        write_json(output_dir / "manifest.json", manifest)
    return finalize_microbenchmark(
        experiment=experiment,
        records=records,
        dimensions=dimensions,
        measurements=measurements,
        output_dir=output_dir,
        manifest=manifest,
    )


def run_figure2(args: argparse.Namespace) -> int:
    output_dir, manifest, spec = performance_context("figure2", args)
    original_profile = spec["experiments"]["figure_2"]["profiles"]["paper"]
    profile = dict(original_profile)
    target = "benchmark_arith_vs_bool"
    step = build_targets([target], output_dir, args)
    if step:
        step["name"] = "build"
        manifest["steps"].append(step)
        manifest["environment_after_build"] = environment_metadata()

    points = paper_micro_points(profile)
    manifest.update({
        "experiment_matrix": profile,
        "paper_experiment_matrix": original_profile,
        "input_scale_locked": True,
        "paper_scale_locked": True,
        "execution_strategy": (
            "Each primitive/elements/width point runs in a fresh three-rank MPI process group. "
            "The runner checkpoints each completed point before launching the next one."
        ),
        "progress": {"completed_points": 0, "total_points": len(points), "current_point": None},
    })
    write_json(output_dir / "manifest.json", manifest)

    records: list[dict[str, Any]] = []
    primitive_labels = {"<": "gt", "==": "eq", "ar": "ar", "mux": "mux", "sort": "sort"}
    for point_index, (primitive, elements, width) in enumerate(points, start=1):
        label = f"{primitive_labels.get(primitive, primitive)}-{elements}-w{width}"
        manifest["progress"] = {
            "completed_points": len(records),
            "total_points": len(points),
            "current_point": {
                "index": point_index, "primitive": primitive,
                "elements": elements, "width": width,
            },
        }
        write_json(output_dir / "manifest.json", manifest)
        print(
            f"[progress] Figure 2 point {point_index}/{len(points)}: "
            f"primitive={primitive} elements={elements} width={width}",
            flush=True,
        )
        params = {
            "nums": elements,
            "sort_nums": elements,
            "widths": width,
            "pmts": primitive,
            "artifact_mode": True,
            "workload_seed": args.seed,
            "batch_size": profile["batch_size"],
        }
        metrics, launch = run_microbenchmark(
            experiment="figure2", executable=target_path(target), params=arg_params(params),
            args=args, output_dir=output_dir, run_name=f"{point_index:03d}-{label}",
            port_offset=point_index - 1,
        )
        if len(metrics) != 1:
            raise RuntimeError(f"{label}: expected exactly one Figure 2 metric, got {len(metrics)}")
        metric = metrics[0]
        observed = (
            str(metric.get("primitive")), int(metric.get("elements", -1)),
            int(metric.get("width", -1)),
        )
        expected = (primitive, elements, width)
        if observed != expected:
            raise RuntimeError(f"{label}: metric identity mismatch: expected={expected}, observed={observed}")
        record = dict(metric)
        record.update({
            "repetition": 1,
            "workload_seed": args.seed,
            "data_scale": float(elements) / float(micro_base_elements(profile, primitive)),
            **launch,
        })
        records.append(record)
        write_json(output_dir / "summary" / "checkpoint.json", records)
        manifest["progress"] = {
            "completed_points": len(records),
            "total_points": len(points),
            "current_point": None,
            "last_completed_point": {
                "index": point_index, "primitive": primitive,
                "elements": elements, "width": width,
            },
            "updated_at": utc_now(),
        }
        write_json(output_dir / "manifest.json", manifest)

    return finalize_microbenchmark(
        experiment="figure2", records=records,
        dimensions=["primitive", "elements", "data_scale", "width"],
        measurements=["arithmetic_ms", "boolean_ms"], output_dir=output_dir,
        manifest=manifest,
    )


def run_figure4(args: argparse.Namespace) -> int:
    output_dir, manifest, spec = performance_context("figure4", args)
    original_profile = spec["experiments"]["figure_4"]["profiles"]["paper"]
    profile = dict(original_profile)
    targets = ["benchmark_jit_single", "benchmark_background_single"]
    step = build_targets(targets, output_dir, args)
    if step:
        step["name"] = "build"
        manifest["steps"].append(step)
        manifest["environment_after_build"] = environment_metadata()
        write_json(output_dir / "manifest.json", manifest)
    manifest["experiment_matrix"] = profile
    manifest["paper_experiment_matrix"] = original_profile
    manifest["input_scale_locked"] = True
    manifest["paper_scale_locked"] = True
    manifest["execution_strategy"] = (
        "Each JIT/background point runs in a fresh three-rank process group so background BMT "
        "queues and task tags cannot leak state between matrix points."
    )
    manifest["background_teardown_policy"] = (
        "A Background point is retained only if it emitted exactly one valid structured metric before "
        "the known exit-134 malloc/MPI_Finalize teardown signature. The observed exit code and warning "
        "remain in the checkpoint; every other nonzero exit fails the experiment."
    )
    points = paper_micro_points(profile)
    total_points = len(points) * repetitions(args)
    manifest["progress"] = {"completed_points": 0, "total_points": total_points, "current_point": None}
    write_json(output_dir / "manifest.json", manifest)

    records: list[dict[str, Any]] = []
    launch_index = 0
    primitive_labels = {"!=": "neq", "<": "gt", "==": "eq", "mux": "mux", "sort": "sort"}
    for repetition in range(1, repetitions(args) + 1):
        for primitive, elements, width in points:
            point_index = len(records) + 1
            point = {
                "index": point_index, "primitive": str(primitive),
                "elements": int(elements), "width": int(width), "repetition": repetition,
            }
            manifest["progress"] = {
                "completed_points": len(records), "total_points": total_points,
                "current_point": point,
            }
            write_json(output_dir / "manifest.json", manifest)
            print(
                f"[progress] Figure 4 point {point_index}/{total_points}: "
                f"primitive={primitive} elements={elements} width={width} phase=JIT+Background",
                flush=True,
            )
            common = {
                "primitive": primitive,
                "num": elements,
                "width": width,
                "batch_size": profile["batch_size"],
                "artifact_mode": True,
                "workload_seed": args.seed,
            }
            label = f"{primitive_labels.get(str(primitive), str(primitive))}-{elements}-w{width}-r{repetition}"
            jit_metrics, jit_launch = run_microbenchmark(
                experiment="figure4", executable=target_path("benchmark_jit_single"),
                params=arg_params(common), args=args, output_dir=output_dir,
                run_name=f"{launch_index:04d}-{label}-jit", port_offset=launch_index,
            )
            launch_index += 1
            background_metrics, background_launch = run_microbenchmark(
                experiment="figure4", executable=target_path("benchmark_background_single"),
                params=arg_params(common), args=args, output_dir=output_dir,
                run_name=f"{launch_index:04d}-{label}-background", port_offset=launch_index,
            )
            launch_index += 1
            if len(jit_metrics) != 1 or len(background_metrics) != 1:
                raise RuntimeError(f"{label}: expected one JIT and one background metric")
            jit_metric, background_metric = jit_metrics[0], background_metrics[0]
            identity = (str(primitive), int(elements), int(width))
            observed_jit = (
                str(jit_metric.get("primitive")), int(jit_metric.get("elements", -1)),
                int(jit_metric.get("width", -1)),
            )
            observed_background = (
                str(background_metric.get("primitive")), int(background_metric.get("elements", -1)),
                int(background_metric.get("width", -1)),
            )
            if observed_jit != identity or observed_background != identity:
                raise RuntimeError(
                    f"{label}: metric identity mismatch: jit={observed_jit}, background={observed_background}"
                )
            records.append({
                "schema_version": 1,
                "experiment": "figure4",
                "primitive": primitive,
                "elements": elements,
                "data_scale": float(elements) / float(micro_base_elements(profile, str(primitive))),
                "width": width,
                "jit_ms": float(jit_metric["jit_ms"]),
                "background_ms": float(background_metric["background_ms"]),
                "repetition": repetition,
                "workload_seed": args.seed,
                "command": {"jit": jit_launch["command"], "background": background_launch["command"]},
                "log": {"jit": jit_launch["log"], "background": background_launch["log"]},
                "launcher_elapsed_seconds": {
                    "jit": jit_launch["launcher_elapsed_seconds"],
                    "background": background_launch["launcher_elapsed_seconds"],
                },
                "observed_return_code": {
                    "jit": jit_launch["return_code"],
                    "background": background_launch["return_code"],
                },
                "background_teardown_failure_ignored": background_launch[
                    "teardown_failure_ignored"
                ],
            })
            write_json(output_dir / "summary" / "checkpoint.json", records)
            manifest["progress"] = {
                "completed_points": len(records), "total_points": total_points,
                "current_point": None, "last_completed_point": point, "updated_at": utc_now(),
            }
            write_json(output_dir / "manifest.json", manifest)
    return finalize_microbenchmark(
        experiment="figure4", records=records,
        dimensions=["primitive", "elements", "data_scale", "width"],
        measurements=["background_ms", "jit_ms"], output_dir=output_dir, manifest=manifest,
    )


def run_figure5(args: argparse.Namespace) -> int:
    return run_micro_figure(
        args, experiment="figure5", spec_key="figure_5", target="benchmark_dyn_batch_size",
        measurements=["execution_ms"],
        dimensions=["primitive", "elements", "data_scale", "width", "batch_size"],
    )


def run_figure7(args: argparse.Namespace) -> int:
    output_dir, manifest, spec = performance_context("figure7", args)
    workloads = spec["workloads"]
    selected = args.workload or list(workloads)
    configurations = args.configuration or ["parsec", "parsec_base"]
    cases = []
    for workload in selected:
        if workload not in workloads:
            raise RuntimeError(f"unknown workload: {workload}")
        workload_spec = workloads[workload]
        params = dict(workload_spec["paper_args"])
        for configuration in configurations:
            for repetition in range(1, repetitions(args) + 1):
                cases.append({
                    "workload": workload, "configuration": configuration,
                    "repetition": repetition, "params": params,
                    "target": workload_spec["cmake_target"],
                    "label": f"{workload}-{configuration}",
                })
    records = run_matrix(cases, args, output_dir, manifest, [case["target"] for case in cases])
    return finalize_performance(
        experiment="figure7", records=records,
        dimensions=["workload", "configuration"], output_dir=output_dir,
        manifest=manifest, x_field="workload", series_field="configuration",
    )


def run_figure8(args: argparse.Namespace) -> int:
    output_dir, manifest, spec = performance_context("figure8", args)
    rows = list(dict.fromkeys(int(value) for value in spec["experiments"]["figure_8"]["rows_grid"]))
    cases = []
    for row_count in rows:
        for repetition in range(1, repetitions(args) + 1):
            cases.append({
                "rows": row_count, "configuration": "parsec", "repetition": repetition,
                "params": {"rows": row_count, "cols": 1}, "target": "db_sort",
                "label": f"rows-{row_count}",
            })
    records = run_matrix(cases, args, output_dir, manifest, ["db_sort"])
    for record in records:
        record["series"] = "parsec"
    return finalize_performance(
        experiment="figure8", records=records, dimensions=["rows", "series"],
        output_dir=output_dir, manifest=manifest, x_field="rows", series_field="series",
    )


def run_table1(args: argparse.Namespace) -> int:
    output_dir, manifest, spec = performance_context("table1", args)
    paper_cases = spec["experiments"]["table_1"]["cases"]
    sizes = [dict(size) for size in paper_cases]
    cases = []
    for size in sizes:
        for mode in ("hash", "nested"):
            for repetition in range(1, repetitions(args) + 1):
                cases.append({
                    **size, "join_mode": mode, "configuration": "parsec", "repetition": repetition,
                    "params": {**size, "hash": mode == "hash"}, "target": "db_join",
                    "label": f"{size['table_num']}-way-{mode}",
                })
    records = run_matrix(cases, args, output_dir, manifest, ["db_join"])
    for record in records:
        record["case"] = f"{record['table_num']}-way/{record['rows']}"
    return finalize_performance(
        experiment="table1", records=records, dimensions=["case", "join_mode"],
        output_dir=output_dir, manifest=manifest, x_field="case", series_field="join_mode",
    )


def run_smoke(args: argparse.Namespace) -> int:
    global ACTIVE_OUTPUT_DIR, ACTIVE_MANIFEST
    output_dir = prepare_result_directory("smoke", args.output_dir)
    manifest = manifest_base("smoke", output_dir, args)
    manifest.update({
        "evaluation_mode": "functional_correctness_only",
        "performance_measurements_collected": False,
    })
    ACTIVE_OUTPUT_DIR, ACTIVE_MANIFEST = output_dir, manifest
    write_json(output_dir / "manifest.json", manifest)
    if not args.skip_build:
        step = run_logged(
            ["sh", "build.sh", f"--{args.build_mode}", f"--simd={args.simd_target}"],
            output_dir / "raw" / "build.log",
        )
        manifest["steps"].append({
            key: value for key, value in step.items()
            if key not in {"output", "elapsed_seconds"}
        })
        if step["return_code"] != 0:
            raise RuntimeError("build failed; see raw/build.log")
        manifest["environment_after_build"] = environment_metadata()
    else:
        missing = [str(target_path(f"exp_{i}")) for i in EXPECTED_SMOKE_IDS if not target_path(f"exp_{i}").is_file()]
        if missing:
            raise RuntimeError("--skip-build requested but smoke binaries are missing")
    verify_command = [
        str(REPO_ROOT / "db" / "exp" / "correctness" / "verify_all.sh"),
        f"--comm={args.comm}", f"--timeout={args.timeout}",
    ]
    if args.comm == "mpi":
        verify_command.append(f"--mpirun={args.mpirun}")
        verify_command.extend(f"--mpi-arg={value}" for value in args.mpi_arg)
    verify = run_logged(verify_command, output_dir / "raw" / "smoke.log")
    manifest["steps"].append({
        key: value for key, value in verify.items()
        if key not in {"output", "elapsed_seconds"}
    })
    passed = sorted({int(value) for value in re.findall(r"^PASS exp_(\d+) \[(?:tcp|mpi)\]$", verify["output"], re.MULTILINE)})
    missing = sorted(set(EXPECTED_SMOKE_IDS) - set(passed))
    succeeded = verify["return_code"] == 0 and not missing
    summary = correctness_summary(passed, missing, args.comm)
    write_json(output_dir / "summary" / "smoke.json", summary)
    manifest.update({"status": summary["status"], "completed_at": utc_now(), "summary": "summary/smoke.json"})
    write_json(output_dir / "manifest.json", manifest)
    print(f"Artifact result: {summary['status']}\nResults written to: {output_dir}")
    return 0 if succeeded else 1


def correctness_summary(passed: list[int], missing: list[int], comm: str) -> dict[str, Any]:
    """Return pass/fail evidence without exposing correctness runs as benchmarks."""
    passed_set = set(passed)
    return {
        "schema_version": 1,
        "experiment": "smoke",
        "evaluation_mode": "functional_correctness_only",
        "status": "passed" if not missing and len(passed_set) == len(EXPECTED_SMOKE_IDS) else "failed",
        "comm": comm,
        "passed_count": len(passed_set),
        "total_checks": len(EXPECTED_SMOKE_IDS),
        "checks": [
            {"experiment": f"exp_{identifier}", "status": "passed" if identifier in passed_set else "failed"}
            for identifier in EXPECTED_SMOKE_IDS
        ],
    }


def run_plot(args: argparse.Namespace) -> int:
    """Regenerate a paper figure/table from one or more existing result sets."""
    manifests: list[tuple[Path, dict[str, Any]]] = []
    csv_paths: list[Path] = []
    experiments: list[str] = []
    profiles: list[str] = []
    for value in args.result_dir or []:
        result_dir = Path(value)
        if not result_dir.is_absolute():
            result_dir = REPO_ROOT / result_dir
        manifest_path = result_dir / "manifest.json"
        if not manifest_path.is_file():
            raise RuntimeError(f"result manifest does not exist: {manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        experiment = str(manifest.get("experiment", ""))
        aggregate = manifest.get("aggregate_csv")
        if not experiment or not aggregate:
            raise RuntimeError(f"manifest has no experiment/aggregate_csv: {manifest_path}")
        manifests.append((manifest_path, manifest))
        experiments.append(experiment)
        profiles.append(str(manifest.get("arguments", {}).get("profile", "")))
        csv_paths.append(result_dir / str(aggregate))
    for value in args.input_csv or []:
        path = Path(value)
        csv_paths.append(path if path.is_absolute() else REPO_ROOT / path)
    experiment = args.experiment or (experiments[0] if experiments else None)
    if not experiment:
        raise RuntimeError("--experiment is required when plotting CSV files without a result directory")
    if any(item != experiment for item in experiments):
        raise RuntimeError(f"cannot merge different experiments: {sorted(set(experiments))}")
    if not csv_paths:
        raise RuntimeError("provide at least one --result-dir or --input-csv")
    if args.output_dir:
        output_dir = Path(args.output_dir)
        if not output_dir.is_absolute():
            output_dir = REPO_ROOT / output_dir
    elif manifests:
        output_dir = manifests[0][0].parent / "figures"
    else:
        raise RuntimeError("--output-dir is required when only --input-csv is used")
    profile = args.profile or (profiles[0] if profiles and len(set(profiles)) == 1 else None)
    try:
        from plotting import generate_plots
    except ModuleNotFoundError:
        from artifact.plotting import generate_plots

    outputs = generate_plots(experiment, csv_paths, output_dir, profile=profile)
    plot_manifest = {
        "schema_version": 1,
        "experiment": experiment,
        "generated_at": utc_now(),
        "profile": profile,
        "source_manifests": [str(path) for path, _ in manifests],
        "source_csvs": [{"path": str(path), "sha256": sha256(path)} for path in csv_paths],
        "plotter": plotter_metadata(experiment),
        "outputs": [str(path) for path in outputs],
    }
    write_json(output_dir / "plot-manifest.json", plot_manifest)
    print(f"Generated {experiment}:\n" + "\n".join(str(path) for path in outputs))
    return 0


def run_doctor(args: argparse.Namespace) -> int:
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: str, required: bool = True) -> None:
        checks.append({"name": name, "passed": passed, "required": required, "detail": detail})

    for command in ("cmake", "ninja", "c++", "mpirun"):
        resolved = capture(["sh", "-c", f"command -v {shlex.quote(command)}"])
        add(f"command:{command}", not resolved.startswith(("unavailable:", "exit code")), resolved)
    try:
        from plotting import configure_plot_cache
    except ModuleNotFoundError:
        from artifact.plotting import configure_plot_cache

    configure_plot_cache()
    for module in ("yaml", "numpy", "matplotlib"):
        try:
            imported = __import__(module)
            add(f"python:{module}", True, getattr(imported, "__version__", "available"))
        except ImportError as exc:
            add(f"python:{module}", False, str(exc))
    try:
        spec = load_spec()
        add("experiment-spec", True, f"schema_version={spec['schema_version']}")
    except RuntimeError as exc:
        add("experiment-spec", False, str(exc))
    add("libsqlparser", (REPO_ROOT / "libsqlparser.so").is_file(), "libsqlparser.so")
    for target in [
        *(f"exp_{index}" for index in EXPECTED_SMOKE_IDS),
        "db_sort", "db_join", "benchmark_arith_vs_bool", "benchmark_jit_single",
        "benchmark_background_single", "benchmark_dyn_batch_size",
    ]:
        path = target_path(target)
        add(f"binary:{target}", path.is_file(), str(path), required=False)
    add(
        "external-baselines", False,
        "ORQ and SECRECY repository commits/patches are not archived in this repository",
        required=False,
    )
    result = {
        "schema_version": 1, "checked_at": utc_now(), "environment": environment_metadata(),
        "checks": checks,
    }
    result["status"] = "passed" if all(item["passed"] for item in checks if item["required"]) else "failed"
    print(json.dumps(result, indent=2))
    if args.json_output:
        path = Path(args.json_output)
        if not path.is_absolute():
            path = REPO_ROOT / path
        path.parent.mkdir(parents=True, exist_ok=True)
        write_json(path, result)
    return 0 if result["status"] == "passed" else 1


def add_common_performance_options(parser: argparse.ArgumentParser) -> None:
    parser.set_defaults(profile="paper", repetitions=FIXED_REPETITIONS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Public workload-data seed.")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--build-mode", choices=("O2", "O3"), default="O3")
    parser.add_argument("--simd-target", choices=("portable", "native", "avx512"), default="native")
    parser.add_argument("--jobs", type=int, default=max(1, min(8, os.cpu_count() or 1)))
    parser.add_argument("--comm", choices=("tcp", "mpi"), default="mpi")
    parser.add_argument(
        "--timeout", type=float,
        help="Per-process timeout in seconds (paper default: 86400).",
    )
    parser.add_argument("--tcp-base-port", type=int, default=24000)
    parser.add_argument("--mpirun", default="mpirun")
    parser.add_argument(
        "--mpi-arg", action="append",
        help="Repeat to replace the default AWS MPI host, mapping, and binding arguments.",
    )
    parser.add_argument("--output-dir")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ParsecDB artifact-evaluation workflows.")
    sub = parser.add_subparsers(dest="command", required=True)

    smoke = sub.add_parser("smoke", help="Build and run all eight correctness checks.")
    smoke.add_argument("--skip-build", action="store_true")
    smoke.add_argument("--comm", choices=("tcp", "mpi"), default="mpi")
    smoke.add_argument("--mpirun", default="mpirun")
    smoke.add_argument(
        "--mpi-arg", action="append",
        help="Repeat to replace the default AWS MPI host, mapping, and binding arguments.",
    )
    smoke.add_argument("--timeout", type=float, default=90)
    smoke.add_argument("--build-mode", choices=("O2", "O3"), default="O3")
    smoke.add_argument("--simd-target", choices=("portable", "native", "avx512"), default="native")
    smoke.add_argument("--output-dir")
    smoke.set_defaults(handler=run_smoke)

    doctor = sub.add_parser("doctor", help="Check build/runtime dependencies and artifact completeness.")
    doctor.add_argument("--json-output")
    doctor.set_defaults(handler=run_doctor)

    plot = sub.add_parser("plot", help="Regenerate or merge paper figures from aggregate CSV results.")
    plot.add_argument("--result-dir", action="append", help="Existing result directory; repeat to merge AWS/HPC runs.")
    plot.add_argument("--input-csv", action="append", help="Normalized aggregate CSV; repeat to add baseline series.")
    plot.add_argument(
        "--experiment",
        choices=("figure2", "figure4", "figure5", "figure7", "figure8", "table1"),
    )
    plot.add_argument("--profile", choices=("quick", "paper"))
    plot.add_argument("--output-dir")
    plot.set_defaults(handler=run_plot)

    figure2 = sub.add_parser("figure2", help="Run arithmetic-vs-boolean sharing microbenchmarks.")
    add_common_performance_options(figure2)
    figure2.set_defaults(handler=run_figure2)

    figure4 = sub.add_parser("figure4", help="Run background-vs-worker-level-JIT BMT microbenchmarks.")
    add_common_performance_options(figure4)
    figure4.set_defaults(handler=run_figure4)

    figure5 = sub.add_parser("figure5", help="Run message-batch-size microbenchmarks.")
    add_common_performance_options(figure5)
    figure5.set_defaults(handler=run_figure5)

    figure7 = sub.add_parser("figure7", help="Run end-to-end workload comparison.")
    add_common_performance_options(figure7)
    figure7.add_argument("--workload", action="append")
    figure7.add_argument("--configuration", action="append", choices=("parsec", "parsec_base", "parsec_noncompact"))
    figure7.set_defaults(handler=run_figure7)

    figure8 = sub.add_parser("figure8", help="Run ParsecDB oblivious sorting points.")
    add_common_performance_options(figure8)
    figure8.set_defaults(handler=run_figure8)

    table1 = sub.add_parser("table1", help="Run multi-way hash/nested join comparison.")
    add_common_performance_options(table1)
    table1.set_defaults(handler=run_table1)

    return parser


def validate_args(args: argparse.Namespace) -> None:
    for name in ("repetitions", "jobs"):
        value = getattr(args, name, None)
        if value is not None and value <= 0:
            raise ValueError(f"--{name.replace('_', '-')} must be positive")
    if hasattr(args, "timeout") and args.timeout is None:
        args.timeout = PAPER_TIMEOUT_SECONDS
    if getattr(args, "timeout", 1) <= 0:
        raise ValueError("--timeout must be positive")
    if hasattr(args, "mpi_arg") and args.comm == "mpi" and not args.mpi_arg:
        args.mpi_arg = list(DEFAULT_MPI_ARGS)


def main() -> int:
    os.chdir(REPO_ROOT)
    parser = build_parser()
    args = parser.parse_args()
    try:
        validate_args(args)
        return args.handler(args)
    except KeyboardInterrupt:
        if ACTIVE_OUTPUT_DIR is not None and ACTIVE_MANIFEST is not None:
            ACTIVE_MANIFEST.update({
                "status": "interrupted",
                "completed_at": utc_now(),
                "error": "interrupted by user",
            })
            try:
                write_json(ACTIVE_OUTPUT_DIR / "manifest.json", ACTIVE_MANIFEST)
            except OSError:
                pass
        print("interrupted by user", file=sys.stderr)
        return 130
    except (RuntimeError, ValueError, OSError, ImportError) as exc:
        if ACTIVE_OUTPUT_DIR is not None and ACTIVE_MANIFEST is not None:
            ACTIVE_MANIFEST.update({"status": "failed", "completed_at": utc_now(), "error": str(exc)})
            try:
                write_json(ACTIVE_OUTPUT_DIR / "manifest.json", ACTIVE_MANIFEST)
            except OSError:
                pass
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
