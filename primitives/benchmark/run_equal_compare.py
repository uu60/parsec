#!/usr/bin/env python3
import argparse
import os
import platform
import shlex
import subprocess
import sys
import time
from typing import List


def run_checked(cmd: List[str]) -> str:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        print(proc.stdout)
        raise RuntimeError(f"command failed: {' '.join(cmd)}")
    return proc.stdout


def run_mpi(args: argparse.Namespace, exe: str) -> str:
    cmd = [
        args.mpirun,
        *shlex.split(args.mpi_extra_args),
        "-np",
        "3",
        exe,
        f"--num={args.num}",
        f"--width={args.width}",
        f"--rounds={args.rounds}",
        f"--warmup={args.warmup}",
    ]
    return run_checked(cmd)


def run_tcp(args: argparse.Namespace, exe: str) -> str:
    procs = []
    try:
        for rank in range(3):
            cmd = [
                exe,
                "--comm_type=tcp",
                f"--tcp_rank={rank}",
                f"--tcp_base_port={args.tcp_base_port}",
                f"--num={args.num}",
                f"--width={args.width}",
                f"--rounds={args.rounds}",
                f"--warmup={args.warmup}",
            ]
            procs.append(subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True))
            time.sleep(0.2)

        outputs = []
        failed = False
        for rank, proc in enumerate(procs):
            out, _ = proc.communicate(timeout=args.timeout)
            outputs.append((rank, proc.returncode, out))
            failed = failed or proc.returncode != 0
        if failed:
            for rank, code, out in outputs:
                print(f"===== tcp rank {rank} exit={code} =====")
                print(out)
            raise RuntimeError("tcp benchmark failed")
        return "\n".join(out for _, _, out in outputs)
    except subprocess.TimeoutExpired:
        for proc in procs:
            proc.kill()
        raise


def print_netem_hint() -> None:
    print("Network shaping is not configured by this runner.")
    print("Use one of these before running the benchmark, then clean it up after.")
    if platform.system() == "Darwin":
        print("")
        print("macOS example with pf/dnctl, requires sudo:")
        print("  echo 'dummynet in quick proto tcp from any to any pipe 1' | sudo pfctl -f -")
        print("  sudo pfctl -e")
        print("  sudo dnctl pipe 1 config bw 1024Mbit/s delay 20ms")
        print("cleanup:")
        print("  sudo pfctl -d")
        print("  sudo dnctl -q flush")
    else:
        print("")
        print("Linux example with tc/netem, requires sudo:")
        print("  sudo tc qdisc add dev lo root handle 1: netem delay 20ms rate 1024mbit")
        print("cleanup:")
        print("  sudo tc qdisc del dev lo root")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--comm", choices=["mpi", "tcp", "both"], default="both")
    parser.add_argument("--exe", default="build/release/primitives/benchmark/benchmark_equal_compare")
    parser.add_argument("--mpirun", default="mpirun")
    parser.add_argument("--mpi-extra-args", default="")
    parser.add_argument("--tcp-base-port", type=int, default=24000)
    parser.add_argument("--num", type=int, default=10000)
    parser.add_argument("--width", type=int, default=64)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--netem-hint", action="store_true")
    args = parser.parse_args()

    if args.netem_hint:
        print_netem_hint()

    exe = os.path.abspath(args.exe)
    if not os.path.exists(exe):
        print(f"executable not found: {exe}", file=sys.stderr)
        return 2

    if args.comm in ("mpi", "both"):
        print("===== MPI =====")
        print(run_mpi(args, exe))

    if args.comm in ("tcp", "both"):
        print("===== TCP =====")
        print(run_tcp(args, exe))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
