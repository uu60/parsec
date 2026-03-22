#!/usr/bin/env python3
"""
Estimate how much Bitwise BMT memory `View::sort` would need if we pre-generated
 the whole sort process in advance.

The model follows the actual code in:
  - db/src/basis/View.cpp
  - primitives/src/compute/batch/bool/*.cpp

Important assumptions:
  - `total_cols` means user/business columns only.
  - `View` adds one extra `$valid` column and one extra `$padding` column.
  - `$valid` participates in row swaps; `$padding` does not.
  - All business columns are assumed to use the same bit width passed by `width`.
  - Sort keys are also assumed to use that same `width`.
  - `View::sort` uses only bitwise BMTs, not arithmetic BMTs.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass


BITWISE_BMT_BYTES = 24  # sizeof(BitwiseBmt) = 3 * int64_t
VALID_COL_WIDTH = 1
DEFAULT_BATCH_SIZE = 1000
DEFAULT_BMT_USAGE_LIMIT = 1


@dataclass
class StageStat:
    k: int
    j: int
    compare_count: int


@dataclass
class Estimate:
    original_rows: int
    padded_rows: int
    business_cols: int
    moved_cols: int
    sort_cols: int
    width: int
    batch_size: int
    stages: int
    compare_pairs_total: int
    bitwise_bmt_uses: int
    unique_pre_generated_bmts: int
    pre_generated_memory_bytes: int
    realtime_peak_bmts: int
    realtime_peak_memory_bytes: int


def ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b


def next_pow2(n: int) -> int:
    if n <= 1:
        return n
    return 1 << (n - 1).bit_length()


def and_bmt_count(num: int, width: int) -> int:
    return ceil_div(num * width, 64)


def less_bmt_count(num: int, width: int) -> int:
    return (math.floor(math.log2(width)) + 3) * and_bmt_count(num, width)


def equal_bmt_count(num: int, width: int) -> int:
    return 2 * less_bmt_count(num, width) + and_bmt_count(num, width)


def mutex_bmt_count(num: int, width: int) -> int:
    return 2 * and_bmt_count(num, width)


def less_live_bmts(num: int, width: int) -> int:
    # Under JIT, BoolLessBatchOperator does several BoolAndBatchOperator calls
    # sequentially, so its peak live BMTs are one BoolAnd batch, not all less BMTs.
    return and_bmt_count(num, width)


def equal_live_bmts(num: int, width: int) -> int:
    # BoolEqualBatchOperator executes:
    #   less(x, y), less(y, x), and(gtv, ltv, 1)
    # sequentially.
    return max(less_live_bmts(num, width), and_bmt_count(num, 1))


def mutex_live_bmts(num: int, width: int) -> int:
    # BoolMutexBatchOperator executes one BoolAndBatchOperator with conditions,
    # which allocates 2 * and_bmt_count(num, width) BitwiseBmts.
    return mutex_bmt_count(num, width)


def simulate_compare_stages(rows: int) -> tuple[int, list[StageStat]]:
    if rows <= 1:
        return rows, []

    padded_rows = next_pow2(rows)
    paddings = [False] * rows + [True] * (padded_rows - rows)
    stages: list[StageStat] = []

    k = 2
    while k <= padded_rows:
        j = k // 2
        while j > 0:
            compare_count = 0
            for i in range(padded_rows):
                l = i ^ j
                if l <= i:
                    continue

                dir_up = (i & k) == 0
                if paddings[i] and paddings[l]:
                    continue

                if (paddings[i] and dir_up) or (paddings[l] and not dir_up):
                    paddings[i], paddings[l] = paddings[l], paddings[i]
                    continue

                if paddings[i] or paddings[l]:
                    continue

                compare_count += 1

            stages.append(StageStat(k=k, j=j, compare_count=compare_count))
            j //= 2
        k *= 2

    return padded_rows, stages


def split_batches(total: int, batch_size: int) -> list[int]:
    if total <= 0:
        return []
    if batch_size <= 0:
        return [total]

    out = []
    done = 0
    while done < total:
        cur = min(batch_size, total - done)
        out.append(cur)
        done += cur
    return out


def estimate_view_sort(
    rows: int,
    total_cols: int,
    sort_cols: int,
    width: int,
    batch_size: int,
    bmt_usage_limit: int,
) -> Estimate:
    if rows < 0:
        raise ValueError("rows must be >= 0")
    if total_cols <= 0:
        raise ValueError("total_cols must be >= 1")
    if sort_cols <= 0:
        raise ValueError("sort_cols must be >= 1")
    if sort_cols > total_cols:
        raise ValueError("sort_cols must be <= total_cols")
    if width <= 0 or width > 64:
        raise ValueError("width must be in [1, 64]")
    if bmt_usage_limit <= 0:
        raise ValueError("bmt_usage_limit must be >= 1")

    padded_rows, stage_stats = simulate_compare_stages(rows)
    moved_cols = total_cols + 1  # business columns + $valid, excluding $padding
    total_bmts = 0
    total_pairs = 0
    realtime_peak_bmts = 0

    for stage in stage_stats:
        c = stage.compare_count
        total_pairs += c
        if c == 0:
            continue

        stage_realtime_peak = 0

        if sort_cols == 1:
            if batch_size <= 0:
                total_bmts += less_bmt_count(c, width)
                total_bmts += mutex_bmt_count(c * moved_cols, width)
                stage_realtime_peak = max(
                    less_live_bmts(c, width),
                    mutex_live_bmts(c * moved_cols, width),
                )
            else:
                batch_peaks = []
                for cnt in split_batches(c, batch_size):
                    total_bmts += less_bmt_count(cnt, width)
                    total_bmts += total_cols * mutex_bmt_count(cnt, width)
                    total_bmts += mutex_bmt_count(cnt, VALID_COL_WIDTH)
                    compare_peak = less_live_bmts(cnt, width)
                    row_swap_peak = (
                        total_cols * mutex_live_bmts(cnt, width)
                        + mutex_live_bmts(cnt, VALID_COL_WIDTH)
                    )
                    batch_peaks.append(max(compare_peak, row_swap_peak))
                stage_realtime_peak = sum(batch_peaks)
        else:
            if batch_size <= 0:
                total_bmts += less_bmt_count(c, width)
                total_bmts += equal_bmt_count(c, width)
                for _ in range(sort_cols - 1):
                    total_bmts += less_bmt_count(c, width)
                    total_bmts += mutex_bmt_count(c, 1)
                    total_bmts += equal_bmt_count(c, width)
                    total_bmts += and_bmt_count(c, 1)
                total_bmts += mutex_bmt_count(c * moved_cols, width)
                compare_peak = max(
                    less_live_bmts(c, width),
                    equal_live_bmts(c, width),
                    mutex_live_bmts(c, 1),
                    and_bmt_count(c, 1),
                )
                row_swap_peak = mutex_live_bmts(c * moved_cols, width)
                stage_realtime_peak = max(compare_peak, row_swap_peak)
            else:
                batch_peaks = []
                for cnt in split_batches(c, batch_size):
                    total_bmts += less_bmt_count(cnt, width)
                    total_bmts += equal_bmt_count(cnt, width)
                    for _ in range(sort_cols - 1):
                        total_bmts += less_bmt_count(cnt, width)
                        total_bmts += mutex_bmt_count(cnt, 1)
                        total_bmts += equal_bmt_count(cnt, width)
                        total_bmts += and_bmt_count(cnt, 1)
                    total_bmts += total_cols * mutex_bmt_count(cnt, width)
                    total_bmts += mutex_bmt_count(cnt, VALID_COL_WIDTH)
                    compare_peak = max(
                        less_live_bmts(cnt, width),
                        equal_live_bmts(cnt, width),
                        mutex_live_bmts(cnt, 1),
                        and_bmt_count(cnt, 1),
                    )
                    row_swap_peak = (
                        total_cols * mutex_live_bmts(cnt, width)
                        + mutex_live_bmts(cnt, VALID_COL_WIDTH)
                    )
                    batch_peaks.append(max(compare_peak, row_swap_peak))
                stage_realtime_peak = sum(batch_peaks)

        realtime_peak_bmts = max(realtime_peak_bmts, stage_realtime_peak)

    unique_bmts = ceil_div(total_bmts, bmt_usage_limit)
    pre_generated_memory_bytes = unique_bmts * BITWISE_BMT_BYTES
    realtime_peak_memory_bytes = realtime_peak_bmts * BITWISE_BMT_BYTES

    return Estimate(
        original_rows=rows,
        padded_rows=padded_rows,
        business_cols=total_cols,
        moved_cols=moved_cols,
        sort_cols=sort_cols,
        width=width,
        batch_size=batch_size,
        stages=len(stage_stats),
        compare_pairs_total=total_pairs,
        bitwise_bmt_uses=total_bmts,
        unique_pre_generated_bmts=unique_bmts,
        pre_generated_memory_bytes=pre_generated_memory_bytes,
        realtime_peak_bmts=realtime_peak_bmts,
        realtime_peak_memory_bytes=realtime_peak_memory_bytes,
    )


def format_bytes(num: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(num)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{num} B"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Estimate Bitwise BMT memory needed by View::sort."
    )
    parser.add_argument("rows", type=int, help="number of rows to sort")
    parser.add_argument("total_cols", type=int, help="number of business columns, excluding $valid/$padding")
    parser.add_argument("sort_cols", type=int, help="number of columns used as sort keys")
    parser.add_argument("width", type=int, help="bit width for each business/sort column")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Conf::BATCH_SIZE; use 0 to model single-batch path (default: 1000)",
    )
    parser.add_argument(
        "--bmt-usage-limit",
        type=int,
        default=DEFAULT_BMT_USAGE_LIMIT,
        help="Conf::BMT_USAGE_LIMIT for background pre-generation (default: 1)",
    )
    parser.add_argument(
        "--show-stages",
        action="store_true",
        help="print compare counts for every bitonic stage",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    estimate = estimate_view_sort(
        rows=args.rows,
        total_cols=args.total_cols,
        sort_cols=args.sort_cols,
        width=args.width,
        batch_size=args.batch_size,
        bmt_usage_limit=args.bmt_usage_limit,
    )

    print("View::sort BMT memory estimate")
    print(f"rows                     : {estimate.original_rows}")
    print(f"padded_rows              : {estimate.padded_rows}")
    print(f"business_cols            : {estimate.business_cols}")
    print(f"moved_cols_in_row_swap   : {estimate.moved_cols} (business + $valid)")
    print(f"sort_cols                : {estimate.sort_cols}")
    print(f"column_width             : {estimate.width}")
    print(f"batch_size               : {estimate.batch_size}")
    print(f"bitonic_stages           : {estimate.stages}")
    print(f"secure_compare_pairs     : {estimate.compare_pairs_total}")
    print(f"bitwise_bmt_uses         : {estimate.bitwise_bmt_uses}")
    print(f"pre_generated_bmts       : {estimate.unique_pre_generated_bmts}")
    print(f"pre_generated_mem_bytes  : {estimate.pre_generated_memory_bytes}")
    print(f"pre_generated_mem_human  : {format_bytes(estimate.pre_generated_memory_bytes)}")
    print(f"realtime_peak_bmts       : {estimate.realtime_peak_bmts}")
    print(f"realtime_peak_mem_bytes  : {estimate.realtime_peak_memory_bytes}")
    print(f"realtime_peak_mem_human  : {format_bytes(estimate.realtime_peak_memory_bytes)}")
    print("")
    print("Notes:")
    print("- The estimate follows View::sort, not Secrets::sort.")
    print("- $padding is padded/swap-tracked during the bitonic network, but it is not mutex-swapped with BMTs.")
    print("- View::sort uses BitwiseBmt only; arithmetic Bmt is 0 here.")
    print("- pre_generated_bmts accounts for BMT reuse through --bmt-usage-limit.")
    print("- realtime_peak_* models the current JIT path: BMTs are generated on demand and freed after each operator.")
    print("- In multi-batch mode, the peak assumes the code's batch futures and per-column mutex futures can overlap.")

    if args.show_stages:
        padded_rows, stage_stats = simulate_compare_stages(args.rows)
        _ = padded_rows
        print("")
        print("Stages:")
        for idx, stage in enumerate(stage_stats, start=1):
            print(f"{idx:3d}. k={stage.k:<6d} j={stage.j:<6d} compare_count={stage.compare_count}")


if __name__ == "__main__":
    main()
