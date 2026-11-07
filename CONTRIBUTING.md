# Contributing to Parsec

Thank you for helping improve Parsec. Contributions to documentation, portability, testing, performance, MPC
primitives, and the database layer are welcome.

## Before opening a change

- Open an issue before making a large API, protocol, or architecture change.
- Do not report suspected vulnerabilities in a public issue; follow [SECURITY.md](SECURITY.md).
- Keep benchmark-only tuning separate from correctness and protocol changes.
- State any change to the trust model, observable metadata, communication pattern, randomness, or reconstruction
  behavior explicitly.

## Development setup

Install a C++17 compiler, CMake 3.22 or newer, Ninja, MPI, OpenSSL, and Boost. GoogleTest (`libgtest-dev` on
Debian/Ubuntu, `googletest` on Homebrew) is needed for the unit tests; without it they are skipped.

Build the core library and correctness tests:

```bash
cmake --preset ci
cmake --build --preset ci
ctest --preset ci
```

Tests live under `tests/` and are labelled by kind:

| Directory | Label | What it runs |
|---|---|---|
| `tests/unit/` | `unit` | single-process GoogleTest cases; finish in seconds |
| `tests/protocol/` | `protocol` (and `mpi` or `tcp`) | three-rank protocol tests, each registered over MPI and TCP |
| `tests/database/` | `database` | `exp_1`..`exp_8` in check mode, compared with expected results |

Run one group with, for example:

```bash
ctest --preset ci -L unit
```

New logic that does not need communication (ring arithmetic, encoding, crypto helpers) should get a GoogleTest case
in `tests/unit/`. A new protocol test is a `.cpp` file in `tests/protocol/` that exits with a non-zero status on any
mismatch; register it with `parsec_add_mpi_test` (and `parsec_add_tcp_test`) in `tests/protocol/CMakeLists.txt`.
Benchmarks go in `benchmarks/` and are not run by CTest.

Build all developer targets:

```bash
cmake --preset dev
cmake --build --preset dev
ctest --preset dev
```

Run the fast correctness suite with AddressSanitizer and UndefinedBehaviorSanitizer:

```bash
cmake --preset ci-sanitizers
cmake --build --preset ci-sanitizers
ASAN_OPTIONS=detect_leaks=0 ctest --preset ci-sanitizers
```

## Pull requests

Keep each pull request focused and include:

1. The problem and intended behavior.
2. Tests or a reproducible validation command.
3. Security and performance implications.
4. Documentation updates for public behavior or configuration changes.

Put library code in the `parsec` namespace; executables (benchmarks, experiments, tests) may use
`using namespace parsec;`. Format C++ changes with `tools/format.sh` (clang-format 21, the version CI checks with
`tools/format.sh --check`). After the one-time bulk reformat, run `git config blame.ignoreRevsFile
.git-blame-ignore-revs` so `git blame` skips it. New correctness tests
must return a non-zero exit status on failure or provide an unambiguous pass/fail result that CTest can validate.

## Commit messages

Use a short imperative subject, for example `Add CTest coverage for bitwise BMT generation`. Add context in the body
when a change affects protocol behavior or makes a non-obvious performance tradeoff.

By contributing, you agree that your contribution is licensed under the repository's Apache License 2.0.
