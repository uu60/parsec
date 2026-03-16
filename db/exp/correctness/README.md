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

- `--mpirun=<cmd>` to specify MPI launcher command.
