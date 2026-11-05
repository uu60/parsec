#!/usr/bin/env python3
"""Launch a Parsec executable as three local TCP ranks and report a combined result.

Example:
    tools/run_tcp_ranks.py --base-port 18000 build/ci/primitives/benchmark/correctness_ot -- --bmt_method=bmt_jit

The exit status is non-zero when any rank fails, times out, or prints a line matching --fail-regex.
"""
import argparse
import re
import subprocess
import sys


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("executable")
    ap.add_argument("args", nargs="*", help="extra arguments passed to every rank (put them after --)")
    ap.add_argument("--base-port", type=int, default=18000, help="rank r listens on BASE_PORT + r")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--timeout", type=float, default=120.0, help="seconds to wait for all ranks")
    ap.add_argument("--fail-regex", default=r"FAIL|MISMATCH", help="output pattern treated as a failure")
    args = ap.parse_args()

    procs = []
    for rank in range(3):
        cmd = [
            args.executable,
            "--comm_type=tcp",
            f"--tcp_rank={rank}",
            "--tcp_size=3",
            f"--tcp_host={args.host}",
            f"--tcp_base_port={args.base_port}",
            *args.args,
        ]
        procs.append(subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True))

    timed_out = False
    outputs = []
    for proc in procs:
        try:
            out, _ = proc.communicate(timeout=args.timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            for other in procs:
                other.kill()
            out, _ = proc.communicate()
        outputs.append(out)

    fail_pattern = re.compile(args.fail_regex) if args.fail_regex else None
    ok = not timed_out
    for rank, (proc, out) in enumerate(zip(procs, outputs)):
        print(f"===== tcp rank {rank} exit={proc.returncode} =====")
        print(out, end="" if out.endswith("\n") else "\n")
        if proc.returncode != 0:
            ok = False
        if fail_pattern and fail_pattern.search(out):
            ok = False

    if timed_out:
        print(f"TIMEOUT after {args.timeout}s", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
