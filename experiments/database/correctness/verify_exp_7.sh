#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
exec "$ROOT_DIR/experiments/database/correctness/verify_exp.py" --exp 7 --bin-dir "$ROOT_DIR/build/experiments/database" "$@"
