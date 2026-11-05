# Security Model

This document records the security boundary of the current Parsec implementation. It is not a proof or an audit.

## Parties and trust assumptions

Parsec runs with three logical ranks:

- Rank 0 and rank 1 are the MPC servers.
- Rank 2 is the client and input data holder.
- The client secret-shares inputs between the two servers.
- Results are reconstructed only to the designated client by the intended API flow.

The current design assumes the two MPC servers do not collude. It is intended for a semi-honest research setting:
parties are expected to follow the protocol while they may attempt to infer information from their local view. The
implementation does not currently claim malicious security, Byzantine fault tolerance, or protection when the two
servers combine their shares.

## Scope

The repository includes Boolean and arithmetic sharing, oblivious transfer, BMT generation, share conversion, secure
operators, and secure relational database operators. Security depends on using compatible primitives, non-overlapping
message tags for concurrent operations, suitable randomness, and the documented party roles.

The MPI and TCP backends transport protocol messages; they should not be treated as encrypted network channels by
default. Deployments must provide appropriate host, network, process, and transport protection.

## Observable information

Depending on the selected operator and application flow, parties or the surrounding system may observe metadata such
as message sizes, batch sizes, timing, operation order, table dimensions, process placement, and failure behavior.
Parsec does not currently claim to hide all access patterns or traffic analysis information.

Application code can also disclose values by reconstructing them to the wrong rank or by logging them. Review calls
to reconstruction APIs and logging paths as part of every security-sensitive change.

## Out of scope for current claims

- Collusion between the two MPC servers.
- Malicious parties that deviate from the protocol.
- Side-channel resistance, including timing, cache, speculative-execution, and memory-access side channels.
- Compromised hosts, compilers, dependencies, operating systems, or random-number sources.
- Denial of service and guaranteed secure erasure.
- Production-grade key management, authentication, authorization, or audit logging.

## Before a production deployment

A production deployment requires a protocol review against the application threat model, independent code audit,
dependency and supply-chain review, authenticated and protected transport, hardened randomness and secret handling,
side-channel analysis, operational monitoring, and an incident-response process.

If code behavior disagrees with this document, treat the discrepancy as a security issue and follow
[SECURITY.md](../SECURITY.md).
