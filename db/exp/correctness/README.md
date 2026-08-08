# DB Exp Correctness Verification

These scripts run each `exp_1` to `exp_8` in `--check=true` mode.
In check mode, each experiment uses a fixed test case and compares against a fixed expected result.

## Build

```bash
cmake --build build --target exp_1 exp_2 exp_3 exp_4 exp_5 exp_6 exp_7 exp_8 -j
```

## Run

Single experiment:

```bash
db/exp/correctness/verify_exp_1.sh
```

Run all:

```bash
db/exp/correctness/verify_all.sh
```

Optional argument:

- `--comm=mpi|tcp` to select the communication backend. Default is `mpi`.
- `--mpirun=<cmd>` to specify MPI launcher command.
- `--mpi-arg=<arg>` to pass a repeatable host, mapping, or binding argument to MPI.
- `--tcp-base-port=<port>` to specify the first TCP port used by TCP mode.
- `--timeout=<seconds>` to set per-process timeout in TCP mode.

MPI mode runs:

```bash
mpirun -np 3 build/db/exp/exp_X --check=true
```

For the two-host paper deployment, repeat `--mpi-arg` for each launcher token.
The host sequence places rank 0 on `parsec0`, rank 1 on `parsec1`, and rank 2
on `parsec0`; no rankfile is used:

```bash
db/exp/correctness/verify_all.sh --comm=mpi \
  --mpi-arg=--bind-to --mpi-arg=none \
  --mpi-arg=--map-by --mpi-arg=seq \
  --mpi-arg=--host --mpi-arg=parsec0,parsec1,parsec0
```

TCP mode starts three local ranks:

```bash
build/db/exp/exp_X --check=true --comm_type=tcp --tcp_rank=0 --tcp_base_port=PORT
build/db/exp/exp_X --check=true --comm_type=tcp --tcp_rank=1 --tcp_base_port=PORT
build/db/exp/exp_X --check=true --comm_type=tcp --tcp_rank=2 --tcp_base_port=PORT
```
