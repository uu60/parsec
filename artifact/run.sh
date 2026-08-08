#!/usr/bin/env sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd -P)

if [ -n "${PARSEC_ARTIFACT_PYTHON:-}" ]; then
  PYTHON_BIN=$PARSEC_ARTIFACT_PYTHON
elif [ -x "$REPO_ROOT/.venv-artifact/bin/python3" ]; then
  PYTHON_BIN=$REPO_ROOT/.venv-artifact/bin/python3
else
  PYTHON_BIN=python3
fi

exec "$PYTHON_BIN" "$SCRIPT_DIR/run.py" "$@"
