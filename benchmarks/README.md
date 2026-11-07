# Benchmarks

Benchmarks are built with `PARSEC_BUILD_BENCHMARKS=ON` (the `release` and `dev` presets) and start three ranks:

```bash
cmake --preset release
cmake --build --preset release
tools/run-local.sh build/release/benchmarks/database/db_sort --rows=1024 --cols=2
```

Options use the runtime's `--name=value` syntax; see the README's Configuration section for the shared options such
as `--comm_type`, `--bmt_method` and `--batch_size`.

## primitives/

| Program | Measures | Main options |
|---|---|---|
| `benchmark_randot_vs_iknpot` | IKNP OT extension throughput (and the insecure random-OT baseline) | `--n --width --rounds --warmup` |
| `benchmark_bmt_comparison` | Scalar vs bitwise BMT generation | |
| `benchmark_equal_compare` | Equality and comparison operators | see `run_equal_compare.py` |
| `benchmark_arith_vs_bool` | Arithmetic vs Boolean sharing for comparison workloads | `--nums --widths --sort_nums` |
| `benchmark_bg_vs_jit` | Background vs just-in-time BMT generation | `--nums --widths --sort_nums` |
| `benchmark_background_single`, `benchmark_jit_single` | One primitive with background or JIT BMTs | `--primitive --num --width` |
| `benchmark_dyn_batch_size` | Effect of the batch size on primitives | `--nums --widths --sort_nums` |

`run_equal_compare.py` and `run_bg_vs_jit_bench.py` run sweeps over the MPI and TCP backends.

## database/

| Program | Measures | Main options |
|---|---|---|
| `db_sort` | Multi-column secure sort | `--rows --cols` |
| `db_join` | Secure join (nested-loop or hash) | `--rows --table_num --hash` |
| `db_join_compare`, `db_join_equal_compare` | Join variants and their comparison operators | see the `run_*.py` scripts |

`run_join_compare.py` and `run_join_equal_compare.py` run these over the MPI and TCP backends.
