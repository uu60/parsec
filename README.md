# Parsec

[![CI](https://github.com/uu60/parsec/actions/workflows/ci.yml/badge.svg)](https://github.com/uu60/parsec/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Parsec** is a C++17 framework for **parallel secure computation** and secure database experiments. It provides MPC primitives, Boolean and arithmetic secret sharing, BMT generation, OT support, communication backends, parallel execution utilities, and secure relational database operators built on a three-party, two-server MPC model.

The name stands for **PARallel SEcure Computing/database**.

> [!IMPORTANT]
> Parsec is research software. Its current security model assumes two non-colluding MPC servers and does not claim
> production readiness. Read the [security model](docs/security-model.md) before using Parsec with sensitive data.

## Contents

- [Execution Model](#execution-model)
- [Quick Start](#quick-start)
- [Features](#features)
- [Project Layout](#project-layout)
- [Dependencies](#dependencies)
- [Build](#build)
- [Run](#run)
- [Configuration](#configuration)
- [Core API](#core-api)
- [Development Notes](#development-notes)
- [License](#license)

## Execution Model

Parsec currently uses three logical ranks:

| Rank | Role |
| --- | --- |
| `0` | MPC server 0 |
| `1` | MPC server 1 |
| `2` | Client and data holder |

The client secret-shares plaintext input to the two servers. Servers execute MPC operators over shares. Results are reconstructed only to the designated client.

Parsec supports two communication backends:

- **MPI**: default backend, suitable for local multi-process runs and cluster execution through `mpirun`.
- **TCP**: socket-based backend, suitable for running the three ranks as independent processes without `mpirun`.

## Quick Start

Install the dependencies listed below, then configure, build, and run the first correctness test:

```bash
cmake --preset ci
cmake --build --preset ci
ctest --preset ci
```

For a full release build including the database components and benchmarks:

```bash
cmake --preset release
cmake --build --preset release
mpirun -np 3 ./build/release/benchmarks/database/db_sort
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow and
[docs/security-model.md](docs/security-model.md) for the protocol assumptions and current limitations.

## Features

- Boolean-share and arithmetic-share MPC primitives.
- Single-value and batch operators for `AND`, `XOR`, equality, less-than comparison, multiplexing, addition, multiplication, and share conversion.
- Boolean and arithmetic secret containers with composable methods.
- BMT generation modes: background, JIT, fixed, and pipeline.
- OT support, including base OT, random OT, and IKNP-style batch OT.
- Communication abstraction with MPI and TCP implementations.
- Parallel execution support through async, CTPL, and optional TBB thread pools.
- Secure relational database operators for tables and views, including projection, filtering, sorting, and join.
- Column-oriented table/view representation for efficient MPC execution.
- Decoupled interface design for extending communication, operators, BMT generators, thread pools, and database components through inheritance or replacement implementations.

## Project Layout

```text
include/parsecdb/        Public headers, included as "parsecdb/<module>/<Header>.h"
  comm/ conf/ parallel/ sync/ utils/     runtime: communication, configuration, threading, utilities
  base/ compute/ intermediate/ ot/       MPC operators, BMT/ROT preparation, oblivious transfer
  secret/ system/ accelerate/            secret containers, runtime lifecycle, SIMD helpers
  db/                                    secure relational tables, views, and operators
src/<module>/            Implementation, one directory per header module
tests/unit/              Single-process GoogleTest cases (label `unit`)
tests/protocol/          Three-party protocol correctness over MPI and TCP (label `protocol`)
tests/database/          Check mode of the database experiments (label `database`)
benchmarks/primitives/   MPC primitive and OT benchmarks, with helper scripts
benchmarks/database/     Secure database operator benchmarks, with helper scripts
experiments/database/    Research experiments exp_1 to exp_8
examples/                find_package example project and MPI hostfile/rankfile templates
tools/                   Launch, formatting, and analysis scripts
cmake/                   CMake package configuration
docs/                    Security model
build.sh                 Convenience build script
```

## Dependencies

Required:

- C++17 compiler
- CMake `3.22+`
- Ninja
- MPI implementation, such as OpenMPI
- OpenSSL
- Boost

Optional:

- TBB, for the `tbb_pool` thread-pool backend
- GoogleTest, for the unit tests in `tests/unit` (skipped when not installed)

macOS example:

```bash
brew install cmake ninja openmpi openssl boost tbb googletest
```

Linux package names vary by distribution. Install the equivalent development packages, for example OpenMPI, OpenSSL headers, Boost, GoogleTest, CMake, and Ninja.

## Build

From the repository root:

```bash
sh build.sh
```

The script:

1. Configures CMake with Ninja.
2. Builds all runtime, primitive, database, benchmark, and experiment targets under `build/`.

Build options:

```bash
sh build.sh --O2
sh build.sh --O3
sh build.sh --asan
```

Manual CMake build:

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

For repeatable configurations, the repository also provides `dev`, `release`, and `ci` CMake presets. The `ci`
preset builds the core library and registered correctness tests without the database components.

### Use from another CMake project

Install Parsec and import it with `find_package`. Headers are installed under `include/parsecdb` with the same
layout as in the source tree, so code includes them as `"parsecdb/utils/Math.h"` both in-tree and after installation.
The C++ namespace, include prefix and CMake package are all named `parsecdb`, which avoids clashes with other
libraries called PaRSEC or Parsec:

```bash
cmake --preset release
cmake --build --preset release
cmake --install build/release --prefix /opt/parsecdb
```

```cmake
find_package(parsecdb 0.1 REQUIRED)
target_link_libraries(my_app PRIVATE parsecdb::parsecdb)  # MPC primitives and runtime
target_link_libraries(my_app PRIVATE parsecdb::db)        # plus the secure database core
```

A complete example is in [examples/find-package](examples/find-package). Pass `-DCMAKE_PREFIX_PATH=/opt/parsecdb` when
configuring it.

## Run

### MPI Backend

MPI is the default communication backend.

Run a three-rank program locally (protocol tests are built by the `dev` and `ci` presets):

```bash
mpirun -np 3 ./build/dev/tests/protocol/correctness_ot
```

Run a database benchmark:

```bash
mpirun -np 3 ./build/benchmarks/database/db_sort
mpirun -np 3 ./build/benchmarks/database/db_join
```

`tools/run-local.sh` starts all three ranks on this machine with either backend. With Open MPI it adds
`--oversubscribe`, so three ranks also start on machines with fewer cores:

```bash
tools/run-local.sh ./build/dev/tests/protocol/correctness_ot
tools/run-local.sh --comm tcp ./build/dev/tests/protocol/correctness_ot
```

Run on multiple machines with an MPI hostfile and rankfile. Templates are in `examples/`; copy them to `hosts` and
`ranks` in the repository root (both are ignored by git) and fill in your addresses:

```bash
cp examples/hosts.example hosts
cp examples/rankfile.example ranks
mpirun -np 3 --hostfile hosts --rankfile ranks ./build/dev/tests/protocol/correctness_ot
```

MPI ranks map directly to Parsec ranks: rank `0` and `1` are servers, rank `2` is the client.

### TCP Backend

TCP mode runs the same three logical ranks as independent OS processes. Use `--comm_type=tcp` and assign each process a unique `--tcp_rank`.

Local example:

```bash
./build/dev/tests/protocol/correctness_ot --comm_type=tcp --tcp_rank=0 --tcp_size=3 --tcp_host=127.0.0.1 --tcp_base_port=18000 &
./build/dev/tests/protocol/correctness_ot --comm_type=tcp --tcp_rank=1 --tcp_size=3 --tcp_host=127.0.0.1 --tcp_base_port=18000 &
./build/dev/tests/protocol/correctness_ot --comm_type=tcp --tcp_rank=2 --tcp_size=3 --tcp_host=127.0.0.1 --tcp_base_port=18000
```

For local runs, `tools/run-local.sh --comm tcp` (a wrapper around `tools/run_tcp_ranks.py`) starts all three ranks,
waits for them, and returns a non-zero status if any rank fails:

```bash
tools/run-local.sh --comm tcp --base-port 18000 ./build/dev/tests/protocol/correctness_ot
```

TCP rank `r` listens on `tcp_base_port + r`. With the default base port, ranks listen on ports `18000`, `18001`, and `18002`.

The current TCP backend uses one IPv4 `tcp_host` value for outbound peer connections and is restricted to `--tcp_size=3`. It is primarily intended for local or controlled single-host experiments; use MPI for general multi-machine placement.

## Configuration

All runtime options use `--name=value` or `--flag` syntax.

Display supported options:

```bash
mpirun -np 3 ./build/dev/tests/protocol/correctness_ot --help
```

Common runtime options:

| Option | Default | Description |
| --- | --- | --- |
| `--comm_type=mpi|tcp` | `mpi` | Communication backend |
| `--tcp_rank=N` | `0` | Logical rank for TCP mode |
| `--tcp_size=N` | `3` | Party count for TCP mode; currently must be `3` |
| `--tcp_host=IP` | `127.0.0.1` | IPv4 host used for outbound TCP peer connections |
| `--tcp_base_port=PORT` | `18000` | Base TCP port; rank `r` uses `PORT + r` |
| `--batch_size=N` | `1000` | Batch size used by operators and database code |
| `--task_tag_bits=N` | `6` | Task/message tag partitioning parameter |
| `--local_threads=N` | hardware concurrency `* 100` | Local worker count |
| `--thread_pool=async|ctpl_pool|tbb_pool` | `async` | Thread-pool implementation |
| `--disable_multi_thread=true|false` | `false` | Disable multi-thread execution |
| `--enable_intra_operator_parallelism=true|false` | `false` | Enable intra-operator parallelism |
| `--enable_simd=true|false` | `true` | Enable SIMD acceleration paths |
| `--enable_transfer_compression=true|false` | `false` | Enable transfer compression |
| `--enable_redundant_ot=true|false` | `true` | Enable redundant OT logic |
| `--enable_class_wise_timing=true|false` | `false` | Collect class-level timing |

BMT options:

| Option | Default | Description |
| --- | --- | --- |
| `--bmt_method=bmt_background|bmt_jit|bmt_fixed|bmt_pipeline` | `bmt_jit` | BMT acquisition strategy |
| `--bmt_pre_gen_seconds=N` | `0` | Background pre-generation time |
| `--max_bmts=N` | `INT_MAX` | Maximum BMT storage |
| `--bmt_usage_limit=N` | `1` | Reuse limit |
| `--bmt_queue_type=cas_queue|lock_free_queue|lock_queue|spsc_queue` | `spsc_queue` | Queue implementation |
| `--bmt_queue_num=N` | `1` | Number of BMT queues |
| `--bmt_gen_batch_size=N` | `10000` | BMT generation batch size |
| `--disable_arith=true|false` | `true` | Disable arithmetic BMT generation |

Database options:

| Option | Default | Description |
| --- | --- | --- |
| `--enable_hash_join=true|false` | `true` | Enable hash-join implementation |
| `--shuffle_bucket_num=N` | `32` | Number of shuffle buckets |
| `--baseline_mode=true|false` | `false` | Disable optimizations for baseline runs |
| `--no_compaction=true|false` | `false` | Disable compaction |
| `--disable_precise_compaction=true|false` | `true` | Disable precise compaction |

## Core API

Parsec is designed around replaceable abstractions rather than fixed implementations. Core modules expose base classes or facades so users can inherit and implement project-specific communication backends, secure operators, BMT/OT providers, thread-pool strategies, and database-layer functionality without rewriting the whole runtime.

All library types and functions live in the `parsecdb` namespace. The examples below assume
`using namespace parsecdb;` (or qualified names such as `parsecdb::System`).

### Runtime Lifecycle

Every MPC executable should initialize and finalize the runtime:

```cpp
#include "parsecdb/utils/System.h"
#include "parsecdb/comm/Comm.h"

using namespace parsecdb;

int main(int argc, char **argv) {
    System::init(argc, argv);

    if (Comm::isClient()) {
        // rank 2 logic
    } else {
        // rank 0 and rank 1 logic
    }

    System::finalize();
    return 0;
}
```

Use `System::nextTask()` to allocate a task tag for an independent operator stream.

### Communication API

`Comm` is the backend-neutral communication facade. It dispatches to `MpiComm` or `TcpComm` based on `--comm_type`.

Common methods:

```cpp
int rank = Comm::rank();
bool server = Comm::isServer();
bool client = Comm::isClient();

Comm::send(value, width, receiverRank, tag);
Comm::receive(value, width, senderRank, tag);

Comm::send(values, width, receiverRank, tag);
Comm::receive(values, width, senderRank, tag);

Comm::send(message, receiverRank, tag);
Comm::receive(message, senderRank, tag);

auto *req = Comm::sendAsync(values, width, receiverRank, tag);
Comm::wait(req);
```

Server-to-server helpers send to or receive from the opposite server rank:

```cpp
Comm::serverSend(value, width, tag);
Comm::serverReceive(value, width, tag);
```

### Secret Sharing API

`Secrets` provides vector-level sharing, reconstruction, and sorting helpers:

```cpp
std::vector<int64_t> shares = Secrets::boolShare(origins, 2, width, task);
std::vector<int64_t> values = Secrets::boolReconstruct(shares, 2, width, task);

std::vector<int64_t> arithShares = Secrets::arithShare(origins, 2, width, task);
std::vector<int64_t> arithValues = Secrets::arithReconstruct(arithShares, 2, width, task);
```

For `boolShare` and `arithShare`, the client rank passes plaintext input. Server ranks pass an empty vector and receive their local shares.

### Secret Object API

`BoolSecret` and `ArithSecret` wrap one shared value and expose composable MPC operations.

```cpp
BoolSecret x(xShare, width, task, 0);
BoolSecret y(yShare, width, task, 0);

BoolSecret z = x.xor_(y);
BoolSecret w = x.and_(y);
BitSecret c = x.lessThan(y);
BoolSecret selected = x.mux(y, c);
BoolSecret opened = selected.reconstruct(2);
```

Arithmetic shares provide:

```cpp
ArithSecret x(xShare, width, task, 0);
ArithSecret y(yShare, width, task, 0);

ArithSecret sum = x.add(y);
ArithSecret product = x.mul(y);
BitSecret less = x.lessThan(y);
ArithSecret selected = x.mux(y, less);
ArithSecret opened = selected.reconstruct(2);
```

Conversion helpers:

```cpp
ArithSecret a = boolSecret.arithmetic();
BoolSecret b = arithSecret.boolean();
```

### Batch Operator API

Batch operators operate on `std::vector<int64_t>` shares. Constructors generally take:

```text
xs, ys, width, taskTag, msgTagOffset, clientRank
```

Example:

```cpp
std::vector<int64_t> xs;
std::vector<int64_t> ys;
int task = System::nextTask();
int width = 32;

BoolAndBatchOperator op(&xs, &ys, width, task, 0, 2);
op.execute()->reconstruct(2);
std::vector<int64_t> result = op._results;
```

Common batch operators:

- `BoolXorBatchOperator`
- `BoolAndBatchOperator`
- `BoolLessBatchOperator`
- `BoolEqualBatchOperator`
- `BoolMutexBatchOperator`
- `BoolToArithBatchOperator`
- `ArithAddBatchOperator`
- `ArithMultiplyBatchOperator`
- `ArithLessBatchOperator`
- `ArithEqualBatchOperator`
- `ArithMutexBatchOperator`
- `ArithToBoolBatchOperator`

When manually parallelizing operators, assign non-overlapping message tag ranges. Use each operator's `tagStride()` when available.

### BMT API

Most users should rely on JIT BMT generation through `--bmt_method=bmt_jit`, which is the default.

Explicit BMT generation is available through classes such as:

- `BmtGenerator`
- `BmtBatchGenerator`
- `BitwiseBmtGenerator`
- `BitwiseBmtBatchGenerator`
- `PipelineBitwiseBmtBatchGenerator`

Example:

```cpp
BitwiseBmtBatchGenerator gen(count, width, task, 0);
gen.execute();
auto &bmts = gen._bmts;
```

Operators that accept pre-generated BMTs expose `setBmts(...)`.

### Database API

The reusable database core is organized around:

- `Database`: owns tables in a selected database.
- `Table`: stores column-oriented secret-shared records and schema metadata.
- `View`: represents relational views over secret-shared columns.
- `Views`: provides relational operations such as projection, filtering, sorting, and hash join.

Use the `db_core` CMake target for direct C++ integration. Research experiments and benchmarks use the core under
`experiments/database` and `benchmarks/database`.

## Development Notes

- The default BMT mode is JIT and is usually the easiest mode for parallel code.
- Background and fixed BMT modes may require preserving BMT consumption order when operators are manually parallelized.
- Message tags isolate parallel operator streams. Reusing tags across concurrent operators can cause incorrect receives.
- Larger batches reduce communication overhead but increase memory usage.
- `baseline_mode=true` intentionally disables several optimizations and is useful for controlled experiments.
- Set `PARSEC_BUILD_DATABASE`, `PARSEC_BUILD_BENCHMARKS`, `PARSEC_BUILD_EXPERIMENTS`, and
  `PARSEC_BUILD_TESTS` to control optional targets.
- Contributions should follow [CONTRIBUTING.md](CONTRIBUTING.md). Security-sensitive reports should follow
  [SECURITY.md](SECURITY.md).

## License

Parsec is licensed under the Apache License 2.0. See [LICENSE](LICENSE).
