#!/usr/bin/env python3
import argparse
import os
import re
import subprocess
import time
from typing import List, Tuple


def run_cmd(cmd: List[str]) -> str:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        print(p.stdout)
        raise RuntimeError(f"command failed: {' '.join(cmd)}")
    return p.stdout


def run_mpi(args: argparse.Namespace, exe: str) -> str:
    return run_cmd([args.mpirun, "-np", "3", exe, "--check=true"])


def run_tcp(args: argparse.Namespace, exe: str) -> str:
    base_port = args.tcp_base_port + args.exp * 10
    procs = []
    outputs = []

    try:
        for rank in range(3):
            cmd = [
                exe,
                "--check=true",
                "--comm_type=tcp",
                f"--tcp_rank={rank}",
                f"--tcp_base_port={base_port}",
            ]
            procs.append(subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            ))
            time.sleep(0.2)

        failed = False
        for rank, proc in enumerate(procs):
            out, _ = proc.communicate(timeout=args.timeout)
            outputs.append((rank, proc.returncode, out))
            if proc.returncode != 0:
                failed = True

        combined = "\n".join(out for _, _, out in outputs)
        if failed:
            for rank, code, out in outputs:
                print(f"===== tcp rank {rank} exit={code} =====")
                print(out)
            raise RuntimeError(f"tcp command failed: {exe}")
        return combined
    except subprocess.TimeoutExpired:
        for proc in procs:
            proc.kill()
        for rank, proc in enumerate(procs):
            out, _ = proc.communicate()
            outputs.append((rank, proc.returncode, out))
        for rank, code, out in outputs:
            print(f"===== tcp rank {rank} exit={code} =====")
            print(out)
        raise RuntimeError(f"tcp command timed out: {exe}")


def parse_scalar(output: str) -> int:
    m = re.search(r"CORRECTNESS_SCALAR\s+(-?\d+)", output)
    if not m:
        raise ValueError("cannot find CORRECTNESS_SCALAR")
    return int(m.group(1))


def parse_rows(output: str) -> List[Tuple[int, ...]]:
    m = re.search(r"CORRECTNESS_BEGIN(.*?)CORRECTNESS_END", output, re.S)
    if not m:
        raise ValueError("cannot find CORRECTNESS_BEGIN/CORRECTNESS_END")
    block = m.group(1)
    rows: List[Tuple[int, ...]] = []
    keep_idx = None
    valid_idx = None
    for line in block.splitlines():
        s = line.strip()
        if not (s.startswith("|") and s.endswith("|")):
            continue
        parts = [x.strip() for x in s.strip("|").split("|")]
        if keep_idx is None:
            if all(not re.fullmatch(r"-?\\d+", p) for p in parts):
                for i, p in enumerate(parts):
                    if p == "$valid":
                        valid_idx = i
                keep_idx = [i for i, p in enumerate(parts) if not p.startswith("$")]
                continue
            keep_idx = list(range(len(parts)))
        if valid_idx is not None and valid_idx < len(parts):
            try:
                if int(parts[valid_idx]) == 0:
                    continue
            except ValueError:
                continue
        picked = [parts[i] for i in keep_idx if i < len(parts)]
        try:
            row = tuple(int(x) for x in picked)
            rows.append(row)
        except ValueError:
            continue
    return rows


def expected_rows(exp_id: int) -> List[Tuple[int, ...]]:
    if exp_id == 1:
        return [(10, 1), (20, 1)]
    if exp_id == 2:
        return [(1,), (3,)]
    if exp_id == 3:
        return [(1,)]
    if exp_id == 4:
        return [(1,), (2,), (3,)]
    if exp_id == 5:
        return [(1,)]
    if exp_id == 6:
        return [(5, 1), (7, 1)]
    if exp_id == 8:
        return [(1, 2), (0, 2), (2, 1)]
    raise ValueError("unexpected exp id")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--exp", type=int, required=True, choices=range(1, 9))
    ap.add_argument("--bin-dir", default="build/db/exp")
    ap.add_argument("--comm", choices=["mpi", "tcp"], default="mpi")
    ap.add_argument("--mpirun", default="mpirun")
    ap.add_argument("--tcp-base-port", type=int, default=21000)
    ap.add_argument("--timeout", type=float, default=60.0)
    args = ap.parse_args()

    exe = os.path.join(args.bin_dir, f"exp_{args.exp}")
    if not os.path.exists(exe):
        print(f"executable not found: {exe}")
        return 2

    if args.comm == "mpi":
        out = run_mpi(args, exe)
    else:
        out = run_tcp(args, exe)

    if args.exp == 7:
        val = parse_scalar(out)
        if val != 44:
            print(f"FAIL exp_7: got {val}, expected 44")
            return 1
        print(f"PASS exp_7 [{args.comm}]")
        return 0

    got = sorted(set(parse_rows(out)))
    exp = sorted(set(expected_rows(args.exp)))
    if got != exp:
        print(f"FAIL exp_{args.exp}")
        print(f"got: {got}")
        print(f"expected: {exp}")
        return 1

    print(f"PASS exp_{args.exp} [{args.comm}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
