# DB Exp Correctness Verification

These scripts run each `exp_1` to `exp_8` in `--check=true` mode.
In check mode, each experiment uses a fixed test case and compares against a fixed expected result.

## Build

```bash
cmake --preset dev
cmake --build --preset dev
```

When `PARSEC_BUILD_TESTS=ON`, these checks are also registered with CTest under the `database` label for both
backends:

```bash
ctest --preset dev -L database
```

## Run

Single experiment:

```bash
experiments/database/correctness/verify_exp_1.sh
```

Run all:

```bash
experiments/database/correctness/verify_all.sh
```

Optional argument:

- `--comm=mpi|tcp` to select the communication backend. Default is `mpi`.
- `--mpirun=<cmd>` to specify MPI launcher command.
- `--bin-dir=<dir>` to point at the directory containing `exp_N`. By default the script uses
  `$PARSEC_BUILD_DIR/experiments/database` when set, otherwise the first existing directory among `build/`,
  `build/release/`, and `build/dev/`.
- `--tcp-base-port=<port>` to specify the first TCP port used by TCP mode.
- `--timeout=<seconds>` to set per-process timeout in TCP mode.

MPI mode runs:

```bash
mpirun -np 3 build/experiments/database/exp_X --check=true
```

TCP mode starts three local ranks:

```bash
build/experiments/database/exp_X --check=true --comm_type=tcp --tcp_rank=0 --tcp_base_port=PORT
build/experiments/database/exp_X --check=true --comm_type=tcp --tcp_rank=1 --tcp_base_port=PORT
build/experiments/database/exp_X --check=true --comm_type=tcp --tcp_rank=2 --tcp_base_port=PORT
```
