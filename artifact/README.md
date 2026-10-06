# ParsecDB Artifact Evaluation

This directory is the evaluation entry point for "ParsecDB: Unlocking Efficient Parallelism in
Two-party Secret-shared Databases" (NSDI '27 submission 76). The runner preserves raw logs,
machine-readable rank-level timings, source and environment provenance, aggregates, and publication
plots in PNG, SVG, and PDF formats.

The primary evaluation format is an author-provisioned, two-node AWS deployment that remains
available for the evaluation period. Reviewers enter through `parsec0`; MPI uses the private network
between `parsec0` and `parsec1`. An immutable source archive is supplied as a recovery copy, but
paper performance should be measured on the provisioned instances rather than in Docker or on an
unrelated machine. Public addresses, SSH authentication material, the expected commit, availability
window, and host-key fingerprints are delivered separately in the artifact-submission page's
access-controlled Description field. They are intentionally not committed to this repository.

## Supported claims and current scope

The repository directly automates the following paper results:

- all eight end-to-end query correctness checks;
- all three panels of Figure 2 (arithmetic versus boolean sharing);
- all three panels of Figure 4 (background versus worker-level JIT BMT generation);
- all three panels of Figure 5 (message batch-size sensitivity);
- the ParsecDB and ParsecDB-base series in Figure 7;
- the ParsecDB series in Figure 8;
- both join modes in Table 1.

The ORQ source is not embedded in this Git repository, but the provided AWS environment contains the
paper revision at `/home/reviewer/orq`. The optional `run_orq.sh` workflow below validates that
revision, configures the required cross-node libOTe hostname, uses real BMT generation, and exports
normalized Figure 7/8 CSVs. SECRECY remains unavailable. An independent archive should still bundle
the exact ORQ revision and patch rather than depend on the temporary AWS environment.

## Getting Started Instructions

The initial doctor and smoke checks below assume the supplied AWS instances and are designed to
finish within 30 minutes. The complete performance workflows can take hours. Do not install
packages, rebuild, change hostnames, or edit network configuration during the initial check: the
toolchain, virtual environment, binaries, hostnames, and private-node SSH are already configured.

### 1. Connect to the entry node

Use the public address, user, private key, expected host-key fingerprint, and expected commit from
the artifact-submission Description:

```bash
ssh -i <PATH_TO_REVIEWER_KEY> <REVIEWER_USER>@<PARSEC0_PUBLIC_IP>
cd ~/parsec
```

The reviewer normally needs to log in only to `parsec0`. That node launches rank 1 on `parsec1`
over the private network. The private key, addresses, username, and fingerprints belong only in the
access-controlled submission Description; do not copy them into this repository or another public
page.

### Reviewer command sheet

After SSH login, all core operations start in the preconfigured checkout:

```bash
cd ~/parsec

# Confirm the submitted revision and verify that no other benchmark is active.
hostname
git rev-parse HEAD
git status --short
pgrep -af '[m]pirun|[b]enchmark_|artifact/run.py|/exp_[1-8] ' || true

# Kick-the-tires environment and correctness checks.
./artifact/run.sh doctor
./artifact/run.sh smoke --skip-build
```

Before any ParsecDB smoke or performance workflow starts, the runner checks both nodes for leftover
ORQ processes. If one is found, it stops with a diagnostic instead of reporting a resource-contended
timing as passed; run `./artifact/run_orq.sh cleanup` and retry.

To apply the same eight expected-result checks to Background BMT generation, use a small bounded
queue suitable for correctness testing:

```bash
./artifact/run.sh smoke --skip-build \
  --bmt-method=bmt_background --max-bmts=10000
```

For a clean end-to-end evaluation, `run_all.sh` includes doctor and smoke before the fixed
paper-scale performance workflows:

```bash
./artifact/run_all.sh
```

After completing the kick-the-tires commands above, reviewers may avoid repeating doctor/smoke by
running the performance workflows individually:

```bash
./artifact/run.sh figure2 --skip-build
./artifact/run.sh figure4 --skip-build
./artifact/run.sh figure5 --skip-build
./artifact/run.sh figure7 --skip-build
./artifact/run.sh figure8 --skip-build
./artifact/run.sh table1 --skip-build
```

Table 1 is the longest workflow. Each secure join prints its metric only after
the whole case completes, so a quiet terminal by itself does not indicate a
hang. The runner records the active case in `manifest.json`. Both join modes
pass `--shuffle_bucket_num=4` explicitly rather than inheriting the source
default. Hash joins use asynchronous task execution with batch size 256.
Nested joins use batch size 1024 with a bounded 18,000-worker CTPL pool for
all four cases, which avoids exceeding the provided AWS node's memory limit.
Both modes disable IKNP-internal
multithreading and enable intra-operator parallelism. These parameters are
fixed in `artifact/experiments.yaml`; the reviewer does not need to pass them
on the command line.

Figures 2, 4, and 5 print every point before launching it and checkpoint every successful point.
To inspect a running experiment from another SSH session:

```bash
latest=$(find artifact/results -maxdepth 1 -type d | sort | tail -n 1)
echo "$latest"
python3 -m json.tool "$latest/manifest.json" | tail -n 40
python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))))' \
  "$latest/summary/checkpoint.json"
```

Each completed workflow writes normalized CSV/JSON data and PNG/SVG/PDF outputs under its new
`artifact/results/<UTC timestamp>-<experiment>/` directory.

### 2. Verify the immutable checkout and idle state

```bash
hostname
git rev-parse HEAD
git status --short
tmux ls 2>/dev/null || echo "no tmux sessions"
pgrep -af '[m]pirun|[b]enchmark_|/exp_[1-8] ' || true
```

Expected results are `parsec0`, the commit listed in the private instructions, an empty
`git status --short`, and no author experiment process. Do not run a second performance experiment
if an author-owned job is present; report it through the anonymous evaluation channel instead.

Verify the preconfigured peer and MPI rank placement:

```bash
getent hosts parsec0 parsec1
ssh -o BatchMode=yes parsec1 hostname
mpirun --bind-to none --map-by seq \
  --host parsec0,parsec1,parsec0 -np 3 hostname
```

The peer command must print `parsec1`. The MPI command must launch two ranks on `parsec0` and one on
`parsec1`; output order is not significant.

MPI is the default communication backend for smoke and every Figure/Table command. The artifact
also supplies the provisioned AWS launcher arguments by default: `--bind-to none --map-by seq
--host parsec0,parsec1,parsec0`. Reviewers therefore do not need to add `--comm` or `--mpi-arg`.
Supplying any `--mpi-arg` replaces the complete AWS default list, which supports an explicit HPC
hostfile or custom placement. Use `--comm=tcp` only for an explicit local three-process fallback.

### 3. Check dependencies and correctness

```bash
./artifact/run.sh doctor
./artifact/run.sh smoke --skip-build
```

Success is indicated by `PASS exp_1` through `PASS exp_8` and `Artifact result: passed`. The command
maps rank 0 to `parsec0`, rank 1 to `parsec1`, and the rank-2 client to `parsec0`. The repeated
hostname is intentional and no rankfile or hostfile is required. Missing ORQ/SECRECY is an optional
external-baseline scope warning, not an environment failure. ORQ is provisioned separately on the
AWS nodes; SECRECY is not included.

Smoke is a functional-correctness check, not a benchmark. Its summary contains only per-experiment
pass/fail status, the communication backend, and the selected BMT method/capacity. It does not report
elapsed time, throughput, output size, or any other performance measurement. Wall-clock measurements
are collected only by the Figure/Table performance commands below.

### 4. Run one paper-scale two-node workload

```bash
./artifact/run.sh figure7 --skip-build \
  --workload=password_reuse --configuration=parsec
```

Every command writes to a new `artifact/results/<UTC timestamp>-<experiment>/` directory. Results
are ignored by Git.

The figure is generated automatically at the end of each performance run. Plotting can also be
repeated from the saved CSV without rebuilding or rerunning the MPC workload:

```bash
./artifact/run.sh plot --result-dir=artifact/results/<timestamp>-figure7
```

This selected-workload run uses the paper input cardinality. Every reviewer command creates a new
timestamped directory and does not overwrite the
pre-existing author results listed in the private access instructions.

## Detailed Instructions

### Provisioned AWS environment

The paper AWS profile consists of two `c5n.4xlarge` instances, one in `us-east-1` and one in
`us-east-2`, running Ubuntu 22.04 and OpenMPI 4.1.2. The logical placement is fixed:

| MPI rank | Host | Role |
| --- | --- | --- |
| 0 | `parsec0` | first MPC server and MPI launcher |
| 1 | `parsec1` | second MPC server |
| 2 | `parsec0` | client/result reconstruction |

The repository is `~/parsec` on both nodes, with matching source, build output, and
`.venv-artifact`. All artifact commands are launched from `parsec0`. Hostnames resolve to private
addresses, and `parsec0` has noninteractive SSH access to `parsec1`. MPI placement is supplied on
the command line so no mutable rankfile or hostfile is part of the experiment.

Run only one performance matrix at a time. Concurrent matrices compete for CPU, memory bandwidth,
and the inter-region link and invalidate timing comparisons. The private access instructions state
the instance availability window and the location of any author-generated reference result
directories. Treat those directories as read-only. New runs under `artifact/results/` use unique
UTC timestamps.

The instances must remain powered on and reachable throughout evaluation. Reviewers are not expected
to stop, reboot, resize, terminate, or reconfigure either instance. If a node becomes unreachable or
the environment differs from the private access sheet, report it through the anonymous evaluation
channel instead of repairing cloud infrastructure.

### Measurement convention

Every Figure/Table C++ benchmark performs an all-rank barrier immediately before and after its measured region.
Each rank emits one JSON `ARTIFACT_METRIC` record. The runner defines a run's elapsed time as the
maximum elapsed time of server ranks 0 and 1. The current artifact collects exactly one measurement
per point, so its aggregate mean equals that measurement and no variance estimate is available.
Input generation and table construction are outside the timed region. Correctness reconstruction is
also excluded from paper runs because `--check` is not enabled.

Figures 2, 4, and 5 retain the original primitive benchmark convention: each emitted point is the
arithmetic mean of the two server-rank elapsed times in milliseconds. With one invocation per point,
the aggregate mean equals that emitted value and the stored standard-deviation field is zero by
construction. Their client process emits normalized `ARTIFACT_MICRO_METRIC` JSON records; legacy
timestamp CSV output is disabled only while `--artifact_mode=true` is active.

`--workload_seed=20270276` controls only public synthetic workload generation. Secret sharing,
protocol masks, OT, and BMT generation continue to use the runtime's independent cryptographic
randomness. The seed, complete command, rank metrics, commit, dirty status, host, OS, CPU, compiler,
MPI version, and specification hash are recorded.

### Fixed evaluation input matrix

All performance commands use paper input cardinalities and exactly one measurement per point.
There is no data-scale, per-command row-count, profile, or repetition override. Manifests record
`input_scale_locked=true` and `paper_scale_locked=true`. `--timeout=SECONDS` changes only the wait
limit and is recorded in the manifest. Figures 2 and 5 default to a stable representative matrix:
it retains every paper point except the optional 2x/64-bit sort stress point(s), which previously
showed extreme scheduler-dependent tails in this 16-vCPU AWS environment. Use `--full-matrix` to
include those stress points. This changes matrix coverage only; it does not change any retained
point's input, implementation, or measurement method.

For Figures 2 and 5, every point has an experiment-specific timeout and one fresh-process retry by
default. A timeout terminates the complete local MPI launcher process group before retrying. Use
`--timeout=SECONDS` to replace the default point budget or `--retries=N` to change the retry count.
Successful points are checkpointed and may be resumed without rerunning them:

```bash
./artifact/run.sh figure5 --skip-build \
  --resume=artifact/results/RESULT_DIR
```

Resume validates the experiment, paper matrix, MPI placement, seed, and exact checkpoint prefix.
The manifest records both the original/resumed commits and specification hashes for auditability.

### Figure 2: arithmetic versus boolean sharing

```bash
./artifact/run.sh figure2
```

The generated three-panel figure compares arithmetic and boolean time at the 1x/64-bit point,
arithmetic-over-boolean slowdown over data scale, and boolean-sharing time over 16/32/64-bit
communication widths. The measurement includes both online execution and JIT BMT generation inside the
primitive calls, matching the paper's timing definition. Inputs are
`100,000/500,000/1,000,000` for non-sort operations and `10,000/50,000/100,000` for sort.
The stable reviewer matrix executes 24 points: every 0.2x/1x primitive point needed by the paper
plot, all 16/32/64-bit middle-size points, and every non-sort 2x point. The optional
`sort/100000/64-bit` stress point makes the `--full-matrix` run 25 points.
Each point runs as an independent MPI process group. Before each launch, the runner prints
`[progress] Figure 2 point N/24`; after every successful point it updates
`summary/checkpoint.json` and the manifest's `progress` object. A slow or failed point is therefore
visible without waiting for the complete matrix, and completed measurements remain auditable.

### Figure 4: background versus worker-level JIT BMTs

```bash
./artifact/run.sh figure4
```

This runs `neq`, `gt`, `eq`, `mux`, and `sort` with both BMT paths and renders raw-time and slowdown
panels. The old standalone Python launcher remains available for development, but evaluator runs
should use `artifact/run.sh` so commands, environment metadata, logs, and checkpoint records are
preserved. Its inputs are `100,000/500,000/1,000,000`; sort uses
`10,000/50,000/100,000`. As in Figure 2, it runs 25 plot-relevant points: all sizes at 64 bits and only
the middle size at 16/32 bits. Each JIT/Background pair prints its point number and is checkpointed
before the next pair starts.

### Figure 5: message batch size

```bash
./artifact/run.sh figure5
```

The fixed matrix uses batch sizes `2^4, 2^6, ..., 2^16`; it does not use the benchmark binary's
legacy decimal defaults. The three panels show all primitives at the 1x/64-bit point, sort over data
scale, and sort over bit width. The middle input is 500,000 elements for non-sort primitives and
50,000 for sort; the sort grid is `10,000/50,000/100,000`.
For every primitive and batch size, the runner measures all retained input sizes at 64 bits and
supplements the middle input size with 16/32-bit points; the shared middle/64-bit point is not
duplicated, and impossible batch/input pairs are skipped. The stable reviewer matrix contains 107
points. `--full-matrix` adds the seven 2x/64-bit sort stress points for a total of 114. Every retained
point is a fresh MPI process group; the terminal and manifest show point progress, and
`summary/checkpoint.json` is updated after every point.

### Figure 7: end-to-end queries

The command uses paper input sizes and one measurement per point:

```bash
./artifact/run.sh figure7
```

The workload mapping is:

| Paper label | Executable | Paper input |
| --- | --- | --- |
| Q6 | `build/db/exp/exp_7` | `rows=630` |
| Q4 | `build/db/exp/exp_6` | `rows1=150`, `rows2=623` |
| Q13 | `build/db/exp/exp_8` | `rows1=15`, `rows2=150` |
| pwd | `build/db/exp/exp_4` | `rows=499` |
| credit | `build/db/exp/exp_5` | `rows=499` |
| comorb. | `build/db/exp/exp_1` | `rows1=500`, `rows2=50` |
| rcdiff | `build/db/exp/exp_2` | `rows=299` |
| aspirin | `build/db/exp/exp_3` | `rows1=299`, `rows2=199` |

The optimized `parsec` and `parsec_noncompact` configurations explicitly pass `batch_size=256`,
matching the empirical sweet spot reported in Section 5.3. `parsec_base` passes
`baseline_mode=true` and `batch_size=0`, which disables compaction, multithreading,
intra-operator parallelism, SIMD, and batching. `parsec_noncompact` is available as an explicit
ablation but is not included by default. The automated paper series uses `parsec_base`, matching
the Figure 7 legend and Section 9.3.

### Figure 8: oblivious sorting

```bash
./artifact/run.sh figure8
```

The fixed matrix evaluates every power of two from 2 through 131,072 rows with one sort column. This grid
was recovered from the plotted points and the Section 9.4 upper limit; that provenance is recorded
in `experiments.yaml`. ORQ-bitonic, ORQ-quick, and ORQ-radix come from an external ORQ source that
is not embedded in this Git repository. The provisioned AWS environment supplies the validated ORQ
revision through the separate data-only workflow described below.

The generated Figure 8 contains the available ParsecDB measurements and explicitly reports that the
external series are not part of that plot. The separate ORQ workflow reports CSV/JSON measurements
only; it does not generate or modify this figure.

### Optional ORQ baseline on the provided AWS nodes

ORQ is intentionally separate from `run.py` and `run_all.sh` because its real-BMT experiments can
take substantially longer than the ParsecDB workflows. It runs only when the reviewer explicitly
invokes `./artifact/run_orq.sh`. See [ORQ.md](ORQ.md) for prerequisites, the short Q6 check, complete
Figure 7/8 commands, output data, and cleanup behavior.

### Table 1: multi-way joins

```bash
./artifact/run.sh table1
```

The fixed matrix runs hash and nested-loop joins for `(tables, rows per table)` equal to `(2,3163)`,
`(3,216)`, `(4,57)`, and `(5,26)`.

The command automatically applies the validated AWS parameters described above. All four nested-loop
cases use the same 18,000-worker CTPL configuration; there is no per-case worker-count override. No
additional join, batch-size, thread-pool, IKNP, or shuffle arguments are required.

### Regenerating and merging plots

`artifact/plotting.py` normalizes aggregate CSV input and dispatches to the vendored
`artifact/parsec_charts/` paper-script adapters. The mapping is `plot_micro_bool.py` (Figure 2),
`plot_micro_bmt.py` (Figure 4), `plot_dyn.py` (Figure 5), `plot_endtoend.py` (Figure 7),
and `plot_sort.py` (Figure 8). No paper values are hard-coded. Every
performance command automatically produces a 300-DPI PNG preview, an editable SVG, and a vector
PDF; the selected script path and hash are recorded in `plot-manifest.json`. Figure 2 and Figure 4
use grouped bars in all three panels, matching the paper scripts.

The plot directory also contains `plot-manifest.json` with source manifest paths, CSV hashes,
plotter hash, profile, and output paths. `--input-csv` accepts additional normalized ParsecDB result
series. Figure 7 CSVs require `workload`, `configuration`, and `mean_elapsed_seconds`; Figure 8 CSVs
require `rows`, `series`, and `mean_elapsed_seconds`. The separate ORQ workflow is data-only and does
not invoke this plotting path.

### AWS convenience workflow

```bash
./artifact/run_all.sh
```

Run this convenience command from `parsec0`; its Figure/Table commands inherit the default AWS MPI
placement. For a single-machine functionality fallback, invoke an individual command with
`--comm=tcp` instead.

This runs doctor, smoke, the paper-scale Figures 2/4/5/7/8, and Table 1. These
single-repetition runs can still take hours.

### Recovery or independent deployment

This section is a fallback for a damaged instance or independent source build; it is not part of the
normal AWS Getting Started workflow. Required native dependencies are CMake 3.22+, Ninja, a C++17
compiler, OpenMPI, OpenSSL, Boost, Readline, and Python 3.10+. On Ubuntu 22.04:

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build openmpi-bin libopenmpi-dev \
  libssl-dev libboost-all-dev libreadline-dev python3 python3-venv
python3 -m venv .venv-artifact
. .venv-artifact/bin/activate
python3 -m pip install -r artifact/requirements.txt
sh build.sh --O3
```

`artifact/run.sh` automatically uses `.venv-artifact/bin/python3` when it exists. Set
`PARSEC_ARTIFACT_PYTHON=/absolute/path/to/python3` only to override it. For a portable
functionality-only fallback, use the Docker image:

```bash
docker build -f artifact/Dockerfile -t parsecdb-artifact .
docker run --rm parsecdb-artifact
```

The Docker entrypoint explicitly selects TCP because the container does not have access to the
provisioned AWS hostnames. Docker is functionality-only and is not used for performance results.

Container or independently provisioned machine timings must not be compared with measurements from
the author-provisioned AWS instances.

### Result layout

```text
artifact/results/<timestamp>-<experiment>/
  manifest.json
  raw/                 complete build and per-rank execution logs
  summary/
    checkpoint.json    completed runs, updated after every point
    <experiment>.json  raw records plus aggregate records (performance experiments)
    smoke.json         correctness status only; no performance measurements
    <experiment>.csv   arithmetic means and standard deviations
    <experiment>-raw.csv
  figures/
    <experiment>.png   300-DPI evaluator preview
    <experiment>.svg   editable vector figure
    <experiment>.pdf   publication-quality vector figure
```

Never edit raw logs after a run. For Figures 2 and 5, preserve the directory and use the documented
`--resume` command to continue a compatible checkpoint. For other workflows, preserve a failed
directory and rerun into a new one. Do not combine results from different commits, dirty states,
seeds, or hardware without explicitly recording that fact.

Aggregate JSON/CSV paths are committed to `manifest.json` before plotting starts. If measurement
finishes but plotting fails (for example because Matplotlib is missing), the manifest has
`data_status: passed` and `status: failed`; install `artifact/requirements.txt` and rerun
`./artifact/run.sh plot --result-dir=...`. The expensive MPC measurements do not need to be rerun.

## Credentials and evaluator access

Do not commit AWS keys, private SSH keys, passwords, session tokens, or unrestricted long-lived
accounts. The AWS console and AWS API are not part of this artifact. Provide a dedicated,
least-privilege SSH account, restrict source IPs when practical, use a temporary key, document the
availability and expiration times, and revoke access after evaluation. Put connection details only
in the private artifact-submission channel, not in this repository or a public registration form.

Before submission, copy `artifact/REVIEWER_ACCESS.template.md` outside the repository, fill it with
the live addresses and fingerprints, and send it privately to the AEC. Do not commit the filled
copy. The source archive, accepted paper, README, and checksum should still be submitted as a
recovery package; SSH access is the primary execution path, not the only immutable copy.

## Scope limitations and release checklist

- ORQ is available only on the temporary pre-provisioned AWS nodes; an immutable standalone archive
  must include its source revision and hostname patch. SECRECY is not included, so complete
  cross-system Figure 7 reproduction is not claimed.
- Before release, retain the completed paper-mode raw outputs, record the archive DOI in the
  submission metadata, and commit every artifact file.
- Before handoff, stop author-owned experiments, verify both nodes have the release commit and a
  clean tracked worktree, make reference results read-only, test the temporary reviewer key from a
  fresh client, and record both SSH host-key fingerprints in the private access sheet.

Create an immutable recovery archive from a clean worktree with:

```bash
./artifact/package.sh
```

The script uses `git archive`, embeds the 12-character commit in the filename, expands the full
immutable commit into `experiments.yaml`, verifies that expansion, and writes a SHA-256 checksum. It
refuses to package modified or untracked files.
