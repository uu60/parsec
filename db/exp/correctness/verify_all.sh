#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
rc=0
for i in 1 2 3 4 5 6 7 8; do
  echo "[verify] exp_${i}"
  if ! "$ROOT_DIR/db/exp/correctness/verify_exp_${i}.sh" "$@"; then
    rc=1
  fi
done
exit $rc
