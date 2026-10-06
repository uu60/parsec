# Optional ORQ Baseline

This workflow is intentionally separate from the default ParsecDB artifact commands. Neither
`artifact/run_all.sh` nor `artifact/run.py` invokes ORQ. The real-BMT experiments can take much
longer, so run them only when explicitly evaluating the external baseline.

## Environment

Run commands as `reviewer` from `/home/reviewer/parsec` while both supplied AWS nodes are active.
Only one reviewer should run ORQ at a time. The pre-provisioned source is
`/home/reviewer/orq` at commit `2d7946a95f6d1d49e020789b70a6cfbdc1198a46`.

The runner fixes two environment-specific issues before benchmarking:

1. The upstream real-BMT libOTe connection defaults to `localhost`. The runner idempotently sets
   `LIBOTE_SERVER_HOSTNAME` to `parsec0`, so parsec1 connects to the remote BMT server.
2. The system `startmpc` cleanup command can leave a one-sided process behind. The artifact uses its
   own failure-safe launcher and cleans both nodes after completion or interruption.

The paper's reported ORQ numbers use the upstream default execution configuration: one worker, one
communication thread per worker, and batch setting `-12`. Accordingly, the runner intentionally
leaves `-T`, `-n`, and `-b` unspecified. All performance commands use two-party NoCopy
communication, real BMT generation, and scale factor `0.0001`. Outputs are kept under
`artifact/results/`; raw logs, a manifest, normalized CSV, and JSON summary are retained together.

The ORQ runner cleans matching processes on both nodes before and after every run, including after
an interruption. ParsecDB artifact commands also refuse to start when an ORQ process remains on
either node, because shared CPU and memory pressure would invalidate their timing.

## Preflight and short check

```bash
./artifact/run_orq.sh doctor
./artifact/run_orq.sh q6
```

Q6 is the shortest paper-scale test. Success requires an `[=SW] Overall` metric and `SQL OK`. The
result directory contains the measured value in `orq-figure7.csv`, together with the raw log and
JSON summary.

## Complete Figure 7

```bash
./artifact/run_orq.sh figure7
```

This runs Q6, Q4, Q13, Password Reuse, Credit Score, Comorbidity, Recurrent C. Diff., and Aspirin
Count sequentially. It does not run any ParsecDB experiment.

## Figure 8 sorting

First run the two-row functional point:

```bash
./artifact/run_orq.sh figure8-smoke
```

This command still exercises all three ORQ sorting implementations. Quick sort and radix sort have
large fixed real-BMT costs, so even this connectivity/correctness check may take substantially
longer than Q6. It is not part of the default ParsecDB smoke test.

The full command evaluates each power of two from 2 through 131,072 rows and can take a long time:

```bash
./artifact/run_orq.sh figure8
```

Each size runs ORQ bitonic, quick, and radix sorting and writes normalized results to
`orq-figure8.csv`.

## Recovery

If a manually launched ORQ command was interrupted, remove only the reviewer-owned ORQ processes:

```bash
./artifact/run_orq.sh cleanup
```

The ORQ workflow reports its measurements as CSV/JSON data and does not generate or modify paper
figures. The external ORQ repository is available only on the temporary AWS nodes; a standalone
archival release must bundle its exact revision and hostname patch rather than depend on this
provisioned path.
