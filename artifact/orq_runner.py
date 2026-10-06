#!/usr/bin/env python3
"""Run the paper's pre-provisioned ORQ baseline on the two AWS nodes."""

from __future__ import annotations

import argparse
import csv
import fcntl
import json
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import sys
import time


PAPER_SCALE_FACTOR = "0.0001"
DEFAULT_BATCH = "-12"
ORQ_COMMIT = "2d7946a95f6d1d49e020789b70a6cfbdc1198a46"
FIGURE7_TARGETS = (
    ("q6", "q6"),
    ("q4", "q4"),
    ("q13", "q13"),
    ("password", "pwd-reuse"),
    ("credit", "credit_score"),
    ("comorbidity", "comorbidity"),
    ("rcdiff", "rcdiff"),
    ("aspirin", "aspirin"),
)
ALL_EXECUTABLES = tuple(target for _, target in FIGURE7_TARGETS) + ("micro_tablesort",)
OVERALL_RE = re.compile(r"\[=SW\]\s+Overall\s+([0-9.eE+-]+)\s+sec")
SORT_RE = re.compile(r"\[\s*SW\]\s+Table (Bitonic Sort|Quicksort|Radix Sort)\s+([0-9.eE+-]+)\s+sec")
HOST_DEFINE_RE = re.compile(r'(#define\s+LIBOTE_SERVER_HOSTNAME\s+")[^"]+(".*)')


class OrqError(RuntimeError):
    pass


def run(
    argv: list[str], *, cwd: Path | None = None, capture: bool = False, check: bool = True,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        check=check,
    )


def ssh(host: str, command: str, *, capture: bool = True, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run(["ssh", host, "bash", "-lc", shlex.quote(command)], capture=capture, check=check)


def require_file(path: Path) -> None:
    if not path.is_file():
        raise OrqError(f"required file not found: {path}")


def configure_hostname(orq_root: Path, server_host: str) -> bool:
    header = orq_root / "include/debug/orq_debug.h"
    require_file(header)
    text = header.read_text(encoding="utf-8")
    match = HOST_DEFINE_RE.search(text)
    if not match:
        raise OrqError(f"LIBOTE_SERVER_HOSTNAME definition not found in {header}")
    current = match.group(0)
    replacement = f'{match.group(1)}{server_host}{match.group(2)}'
    if current == replacement:
        return False
    header.write_text(text[: match.start()] + replacement + text[match.end() :], encoding="utf-8")
    return True


def resolve_non_loopback(host: str) -> str:
    addresses = {item[4][0] for item in socket.getaddrinfo(host, None, socket.AF_INET)}
    usable = sorted(address for address in addresses if not address.startswith("127."))
    if not usable:
        raise OrqError(f"{host!r} does not resolve to a non-loopback IPv4 address")
    return usable[0]


def check_remote_resolution(hosts: tuple[str, str], server_host: str, expected_ip: str) -> None:
    for host in hosts:
        result = ssh(host, f"getent ahostsv4 {shlex.quote(server_host)} | awk 'NR==1 {{print $1}}'")
        actual = (result.stdout or "").strip()
        if actual != expected_ip:
            raise OrqError(f"{host}: {server_host} resolves to {actual!r}, expected {expected_ip!r}")


def active_processes(hosts: tuple[str, str], executables: tuple[str, ...]) -> list[str]:
    found: list[str] = []
    for host in hosts:
        for executable in executables:
            result = ssh(host, f"pgrep -a -x {shlex.quote(executable)} || true")
            output = (result.stdout or "").strip()
            if output:
                found.append(f"{host}: {output}")
    return found


def cleanup(hosts: tuple[str, str], executables: tuple[str, ...]) -> None:
    for host in hosts:
        for executable in executables:
            ssh(host, f"pkill -TERM -x {shlex.quote(executable)} 2>/dev/null || true", capture=False)
    time.sleep(2)
    remaining = active_processes(hosts, executables)
    if remaining:
        raise OrqError("processes remain after SIGTERM:\n" + "\n".join(remaining))


def doctor(args: argparse.Namespace) -> None:
    orq_root = args.orq_dir.resolve()
    require_file(orq_root / "scripts/run_experiment.sh")
    require_file(orq_root / "include/debug/orq_debug.h")
    require_file(Path(__file__).resolve().parent / "orq_bin/startmpc")
    commit = run(["git", "rev-parse", "HEAD"], cwd=orq_root, capture=True).stdout.strip()
    if commit != ORQ_COMMIT:
        raise OrqError(f"ORQ commit is {commit}, expected {ORQ_COMMIT}")
    ip = resolve_non_loopback(args.server_host)
    check_remote_resolution(args.hosts, args.server_host, ip)
    for host in args.hosts:
        ssh(host, "true")
    print(f"PASS ORQ commit: {commit}")
    print(f"PASS libOTe server: {args.server_host} -> {ip}")
    print(f"PASS parties: {','.join(args.hosts)}")
    print("PASS configuration: 2PC + NoCopy + real BMT")


def stream_command(argv: list[str], *, cwd: Path, env: dict[str, str], log_path: Path) -> tuple[int, str]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    process = subprocess.Popen(
        argv,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
    )
    chunks: list[str] = []
    assert process.stdout is not None
    with log_path.open("w", encoding="utf-8") as log:
        for line in process.stdout:
            print(line, end="", flush=True)
            log.write(line)
            log.flush()
            chunks.append(line)
    return process.wait(), "".join(chunks)


def experiment_command(args: argparse.Namespace, target: str, *, rows_exp: int | None = None) -> list[str]:
    command = [
        "bash", "../scripts/run_experiment.sh",
        "-s", "lan",
        "-x", args.host_prefix,
        "-p", "2",
        "-c", "nocopy",
        "-e", "1",
        "-m", "-DTRIPLES=REAL",
    ]
    if rows_exp is None:
        command.extend(["-f", PAPER_SCALE_FACTOR])
    else:
        command.extend(["-r", str(rows_exp)])
    command.append(target)
    return command


def prepare(args: argparse.Namespace) -> tuple[Path, dict[str, str], dict[str, object]]:
    orq_root = args.orq_dir.resolve()
    doctor(args)
    changed = configure_hostname(orq_root, args.server_host)
    cleanup(args.hosts, ALL_EXECUTABLES)
    env = os.environ.copy()
    env["PATH"] = f"{Path(__file__).resolve().parent / 'orq_bin'}:{env.get('PATH', '')}"
    metadata: dict[str, object] = {
        "schema_version": 1,
        "orq_commit": ORQ_COMMIT,
        "orq_root": str(orq_root),
        "hosts": list(args.hosts),
        "libote_server_hostname": args.server_host,
        "hostname_definition_changed": changed,
        "protocol": "2pc",
        "communicator": "nocopy",
        "triples": "REAL",
        "scale_factor": PAPER_SCALE_FACTOR,
        "orq_script_defaults": {
            "threads": 1,
            "communication_threads": "one per worker",
            "batch": DEFAULT_BATCH,
        },
    }
    return orq_root, env, metadata


def run_query(args: argparse.Namespace, label: str, target: str, run_dir: Path, orq_root: Path, env: dict[str, str]) -> float:
    print(f"\n=== ORQ Figure 7: {label} ({target}) ===", flush=True)
    rc, output = stream_command(
        experiment_command(args, target),
        cwd=orq_root / "build",
        env=env,
        log_path=run_dir / "raw" / f"{label}.log",
    )
    matches = OVERALL_RE.findall(output)
    if not matches:
        raise OrqError(f"{label}: missing [=SW] Overall metric (exit code {rc})")
    if "SQL OK" not in output:
        raise OrqError(f"{label}: timing was printed but SQL correctness did not pass")
    return float(matches[-1])


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_figure7(args: argparse.Namespace, targets: tuple[tuple[str, str], ...]) -> Path:
    orq_root, env, metadata = prepare(args)
    run_dir = args.result_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    write_json(run_dir / "manifest.json", {**metadata, "experiment": "orq-figure7"})
    rows: list[dict[str, object]] = []
    try:
        for label, target in targets:
            elapsed = run_query(args, label, target, run_dir, orq_root, env)
            rows.append({
                "workload": label,
                "configuration": "orq_2pc_real_bmt",
                "mean_elapsed_seconds": elapsed,
            })
            print(f"PASS {label}: {elapsed:.6f} seconds", flush=True)
    finally:
        cleanup(args.hosts, ALL_EXECUTABLES)
    csv_path = run_dir / "orq-figure7.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=("workload", "configuration", "mean_elapsed_seconds"))
        writer.writeheader()
        writer.writerows(rows)
    write_json(run_dir / "summary.json", rows)
    print(f"Results: {run_dir}")
    return run_dir


def run_figure8(args: argparse.Namespace) -> Path:
    orq_root, env, metadata = prepare(args)
    run_dir = args.result_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    write_json(run_dir / "manifest.json", {**metadata, "experiment": "orq-figure8", "row_exponents": args.row_exponents})
    rows: list[dict[str, object]] = []
    labels = {"Bitonic Sort": "orq_bitonic", "Quicksort": "orq_quick", "Radix Sort": "orq_radix"}
    try:
        for exponent in args.row_exponents:
            count = 1 << exponent
            print(f"\n=== ORQ Figure 8: {count} rows ===", flush=True)
            rc, output = stream_command(
                experiment_command(args, "micro_tablesort", rows_exp=exponent),
                cwd=orq_root / "build",
                env=env,
                log_path=run_dir / "raw" / f"rows-{count}.log",
            )
            matches = SORT_RE.findall(output)
            if len(matches) != 3:
                raise OrqError(f"rows={count}: expected three sort metrics, got {len(matches)} (exit code {rc})")
            for name, elapsed in matches:
                rows.append({"rows": count, "series": labels[name], "mean_elapsed_seconds": float(elapsed)})
    finally:
        cleanup(args.hosts, ALL_EXECUTABLES)
    csv_path = run_dir / "orq-figure8.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=("rows", "series", "mean_elapsed_seconds"))
        writer.writeheader()
        writer.writerows(rows)
    write_json(run_dir / "summary.json", rows)
    print(f"Results: {run_dir}")
    return run_dir


def default_result_dir(name: str) -> Path:
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return Path("artifact/results") / f"{stamp}-{name}"


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("command", choices=("doctor", "cleanup", "q6", "figure7", "figure8-smoke", "figure8"))
    result.add_argument("--orq-dir", type=Path, default=Path(os.environ.get("ORQ_ROOT", "/home/reviewer/orq")))
    result.add_argument("--server-host", default="parsec0")
    result.add_argument("--host-prefix", default="parsec")
    result.add_argument("--hosts", default="parsec0,parsec1")
    result.add_argument("--result-dir", type=Path)
    return result


def main() -> int:
    args = parser().parse_args()
    args.hosts = tuple(item.strip() for item in args.hosts.split(","))
    if len(args.hosts) != 2:
        raise OrqError("exactly two --hosts are required")
    lock_path = Path("/tmp/parsec-orq-artifact.lock")
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise OrqError("another ORQ artifact run is active") from error
        if args.command == "doctor":
            doctor(args)
        elif args.command == "cleanup":
            cleanup(args.hosts, ALL_EXECUTABLES)
            print("All ORQ artifact processes stopped.")
        elif args.command == "q6":
            args.result_dir = args.result_dir or default_result_dir("orq-q6")
            try:
                run_figure7(args, (("q6", "q6"),))
            finally:
                cleanup(args.hosts, ALL_EXECUTABLES)
        elif args.command == "figure7":
            args.result_dir = args.result_dir or default_result_dir("orq-figure7")
            try:
                run_figure7(args, FIGURE7_TARGETS)
            finally:
                cleanup(args.hosts, ALL_EXECUTABLES)
        else:
            args.row_exponents = [1] if args.command == "figure8-smoke" else list(range(1, 18))
            args.result_dir = args.result_dir or default_result_dir(f"orq-{args.command}")
            try:
                run_figure8(args)
            finally:
                cleanup(args.hosts, ALL_EXECUTABLES)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except OrqError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
