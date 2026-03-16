#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
exec "$ROOT_DIR/db/exp/correctness/verify_exp.py" --exp 5 --bin-dir "$ROOT_DIR/build/db/exp" "$@"
