#!/usr/bin/env python3
import argparse
import os
import shlex
import subprocess
import sys
import time
from typing import List


def run_checked(cmd: List[str], timeout: float) -> str:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
    if proc.returncode != 0:
        print(proc.stdout)
        raise RuntimeError(f"command failed: {' '.join(cmd)}")
    return proc.stdout


def common_args(args: argparse.Namespace) -> List[str]:
    return [
        f"--rows={args.rows}",
        f"--table_num={args.table_num}",
        f"--width={args.width}",
        f"--rounds={args.rounds}",
        f"--warmup={args.warmup}",
        f"--batch_size={args.batch_size}",
        f"--hash={str(args.hash).lower()}",
        f"--shuffle_bucket_num={args.shuffle_bucket_num}",
    ]


def run_mpi(args: argparse.Namespace, exe: str) -> str:
    cmd = [
        args.mpirun,
        *shlex.split(args.mpi_extra_args),
        "-np",
        "3",
        exe,
        *common_args(args),
    ]
    return run_checked(cmd, args.timeout)


def run_tcp(args: argparse.Namespace, exe: str) -> str:
    procs = []
    try:
        for rank in range(3):
            cmd = [
                exe,
                "--comm_type=tcp",
                f"--tcp_rank={rank}",
                f"--tcp_base_port={args.tcp_base_port}",
                *common_args(args),
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--comm", choices=["mpi", "tcp", "both"], default="both")
    parser.add_argument("--exe", default="build/release/benchmarks/database/db_join_compare")
    parser.add_argument("--mpirun", default="mpirun")
    parser.add_argument("--mpi-extra-args", default="")
    parser.add_argument("--tcp-base-port", type=int, default=26200)
    parser.add_argument("--rows", type=int, default=100)
    parser.add_argument("--table-num", "--table_num", dest="table_num", type=int, default=2)
    parser.add_argument("--width", type=int, default=64)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--batch-size", "--batch_size", dest="batch_size", type=int, default=1000)
    parser.add_argument("--hash", action="store_true")
    parser.add_argument("--shuffle-bucket-num", "--shuffle_bucket_num", dest="shuffle_bucket_num", type=int, default=32)
    parser.add_argument("--timeout", type=float, default=300.0)
    args = parser.parse_args()

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
