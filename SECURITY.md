# Security Policy

## Research software status

Parsec is research software and is not currently claimed to be production-ready or independently audited. Passing
the correctness tests does not constitute a security review. Do not use the current implementation to protect
sensitive production data without an independent cryptographic and implementation audit.

## Reporting a vulnerability

Please do not open a public GitHub issue for a suspected vulnerability. Use GitHub's private vulnerability reporting
feature for this repository when it is available. If private reporting is unavailable, contact the maintainers through
the non-public contact method listed on their GitHub profiles and include `Parsec security report` in the subject.

Include the affected commit, configuration and communication backend, a minimal reproduction when safe, the expected
impact, and whether the report may be shared with other maintainers. Avoid including real secret data.

The project does not yet promise a fixed response SLA. A maintainer will acknowledge a report, reproduce it, assess
its impact, and coordinate disclosure and a release when appropriate.

## Supported versions

Until the first tagged release, security fixes are made on the `main` branch. After versioned releases begin, this
section will identify the supported release lines.

For the protocol assumptions and known leakage boundaries, see [docs/security-model.md](docs/security-model.md).
