# ParsecDB Private Reviewer Access

Fill this document outside the public repository and send it only through the artifact evaluation
system's private channel. Replace every angle-bracketed value. Do not give reviewers AWS console or
AWS API credentials.

## Availability

- Access begins: `<UTC DATE AND TIME>`
- Access ends: `<UTC DATE AND TIME>`
- Anonymous support channel: `<AEC CHANNEL OR CONTACT METHOD>`

The two instances will remain powered on throughout this window. Reviewers should not stop, reboot,
resize, terminate, or otherwise reconfigure them.

## Entry node

- Public address: `<PARSEC0_PUBLIC_IP_OR_DNS>`
- SSH user: `<REVIEWER_USER>`
- SSH port: `<PORT; NORMALLY 22>`
- Authentication: `<AEC-PROVIDED PUBLIC KEY OR TEMPORARY REVIEWER KEY>`
- Ed25519 host-key fingerprint: `<SHA256:FINGERPRINT>`
- Private hostname: `parsec0`

Example connection command:

```bash
chmod 600 <PATH_TO_REVIEWER_KEY>
ssh -i <PATH_TO_REVIEWER_KEY> -p <PORT> <REVIEWER_USER>@<PARSEC0_PUBLIC_IP_OR_DNS>
cd ~/parsec
```

Reviewers normally do not need a direct public connection to `parsec1`; `parsec0` launches MPI rank
1 over the private network using preconfigured noninteractive SSH.

## Peer node

- Public address, if direct emergency access is enabled: `<PARSEC1_PUBLIC_IP_OR_DNS_OR_NOT_PROVIDED>`
- SSH user: `<REVIEWER_USER>`
- Ed25519 host-key fingerprint: `<SHA256:FINGERPRINT>`
- Private hostname: `parsec1`

## Immutable software identity

- Repository path on both nodes: `~/parsec`
- Expected full Git commit: `<40_HEX_COMMIT>`
- Source archive filename: `<ARCHIVE.tar.gz>`
- Source archive SHA-256: `<SHA256>`
- Accepted paper filename: `<PAPER.pdf>`

After logging in, verify:

```bash
cd ~/parsec
git rev-parse HEAD
git status --short
ssh -o BatchMode=yes parsec1 'cd ~/parsec && git rev-parse HEAD && git status --short'
```

Both commits must match the expected value and both status commands must print nothing.

## Expected runtimes on this deployment

Fill these values from the final clean rehearsal. State the repetitions and data scale for every
estimate; do not describe a one-repetition or scaled diagnostic as paper reproduction.

| Workflow | Profile/scale/repetitions | Expected wall time |
| --- | --- | --- |
| Getting Started through correctness | quick / 1.0 / 1 | `<DURATION>` |
| Figure 2 | paper / 1.0 / `<REPETITIONS>` | `<DURATION>` |
| Figure 4 | paper / 1.0 / `<REPETITIONS>` | `<DURATION>` |
| Figure 5 | paper / 1.0 / `<REPETITIONS>` | `<DURATION>` |
| Figure 7 | paper / 1.0 / `<REPETITIONS>` | `<DURATION>` |
| Figure 8 | paper / 1.0 / `<REPETITIONS>` | `<DURATION>` |
| Table 1 | paper / 1.0 / `<REPETITIONS>` | `<DURATION>` |

## Result locations and handoff state

- Read-only author reference results: `<ABSOLUTE_PATH_OR_NOT_INCLUDED>`
- New reviewer results: `~/parsec/artifact/results/<UTC timestamp>-<experiment>/`
- Expected handoff state: no author-owned `tmux`, `mpirun`, or benchmark process is running
- Estimated free disk space on `parsec0`: `<VALUE>`
- Estimated free disk space on `parsec1`: `<VALUE>`

If an author experiment is still active, a node is unreachable, a fingerprint differs, or either
checkout is dirty, do not repair or terminate anything. Report the observation through the anonymous
support channel.

Continue with `artifact/README.md`, beginning at **Getting Started Instructions**.
